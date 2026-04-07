//! Vector Snapshot Serialization
//!
//! This module provides snapshot serialization and deserialization for vector data.
//!
//! ## Snapshot Format (Version 0x01)
//!
//! ```text
//! [Version: u8]
//! [Collection Count: u32 LE]
//! For each collection:
//!   [Header Length: u32 LE]
//!   [Header: MessagePack CollectionSnapshotHeader]
//!   For each vector (in VectorId order):
//!     [VectorId: u64 LE]
//!     [Key Length: u32 LE]
//!     [Key: UTF-8 bytes]
//!     [Embedding: dimension * f32 LE]
//!     [Has Metadata: u8 (0 or 1)]
//!     If has metadata:
//!       [Metadata Length: u32 LE]
//!       [Metadata: JSON bytes]
//! ```
//!
//! ## Design Notes
//!
//! 1. **Deterministic Output**: Collections and vectors are written in sorted order
//!    to ensure byte-identical snapshots for the same logical state.
//!
//! 2. **Critical State**: next_id and free_slots MUST be persisted and restored
//!    to maintain VectorId uniqueness across restarts (Invariant T4).
//!
//! 3. **Embedding Format**: Raw f32 LE for efficiency. No compression currently.

use crate::{
    CollectionId, DistanceMetric, IndexBackendFactory, StorageDtype, VectorConfig, VectorError,
    VectorId, VectorRecord, VectorResult, VectorStore,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use strata_core::value::Value;
use strata_core::BranchId;

/// Snapshot format version.
///
/// v0x02 added per-collection `space` to `CollectionSnapshotHeader`.
/// Older v0x01 snapshots are rejected outright (no backward-compat decoder).
pub const VECTOR_SNAPSHOT_VERSION: u8 = 0x02;

/// Collection snapshot header (MessagePack serialized)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSnapshotHeader {
    /// Branch ID for this collection
    pub branch_id: BranchId,
    /// Space the collection lives in (e.g. `"default"`, `"tenant_a"`,
    /// `"_system_"`). Pre-fix snapshots that omitted this field are
    /// rejected by the version check.
    pub space: String,
    /// Collection name
    pub name: String,
    /// Embedding dimension
    pub dimension: usize,
    /// Distance metric (as byte)
    pub metric: u8,
    /// Storage data type (as byte)
    pub storage_dtype: u8,
    /// CRITICAL: Must be persisted to maintain ID uniqueness across restarts
    pub next_id: u64,
    /// CRITICAL: Must be persisted for correct slot reuse after recovery
    pub free_slots: Vec<usize>,
    /// Number of vectors in this collection
    pub count: u32,
    /// Index type: 0 = BruteForce (default), 1 = HNSW
    #[serde(default)]
    pub index_type: u8,
    /// HNSW graph state (serialized bytes, only present for index_type=1)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hnsw_graph_state: Vec<u8>,
}

impl VectorStore {
    /// Serialize vector data for snapshot
    ///
    /// Format:
    /// - Version byte (0x01)
    /// - Collection count (u32 LE)
    /// - For each collection:
    ///   - Header length (u32 LE)
    ///   - Header (MessagePack)
    ///   - For each vector (in VectorId order):
    ///     - VectorId (u64 LE)
    ///     - Key length (u32 LE)
    ///     - Key (UTF-8 bytes)
    ///     - Embedding (dimension * f32 LE)
    ///     - Has metadata (u8: 0 or 1)
    ///     - If has metadata: Metadata length (u32 LE) + Metadata (JSON bytes)
    pub fn snapshot_serialize<W: Write>(&self, writer: &mut W) -> VectorResult<()> {
        // Version byte
        writer
            .write_u8(VECTOR_SNAPSHOT_VERSION)
            .map_err(|e| VectorError::Io(e.to_string()))?;

        let state = self.backends()?;
        let collection_count = state.backends.len() as u32;
        writer
            .write_u32::<LittleEndian>(collection_count)
            .map_err(|e| VectorError::Io(e.to_string()))?;

        // Collect and sort collections for deterministic output
        let mut collection_ids: Vec<_> = state
            .backends
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        // Sort by `(branch_id, space, name)` for deterministic snapshot
        // output. Two collections with the same `(branch_id, name)` in
        // different spaces are distinct entities and must serialize in
        // a stable order.
        collection_ids.sort_by(|a, b| {
            a.branch_id
                .as_bytes()
                .cmp(b.branch_id.as_bytes())
                .then(a.space.cmp(&b.space))
                .then(a.name.cmp(&b.name))
        });

        for collection_id in &collection_ids {
            let backend = state.backends.get(collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection_id.name.clone(),
                }
            })?;
            // Get config from the collection info (which gets it from KV)
            // Use "default" space for snapshot serialization (backwards compat)
            let config = self
                .get_collection(
                    collection_id.branch_id,
                    &collection_id.space,
                    &collection_id.name,
                )?
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection_id.name.clone(),
                })?
                .value
                .config;

            // Get snapshot state from backend
            let (next_id, free_slots) = backend.snapshot_state();

            // Determine index type and serialize graph state if HNSW
            let (index_type, hnsw_graph_state) = {
                let type_name = backend.index_type_name();
                match type_name {
                    "hnsw" => (1u8, Vec::new()),
                    "segmented_hnsw" => (2u8, Vec::new()),
                    _ => (0u8, Vec::new()),
                }
            };

            // Create header
            let header = CollectionSnapshotHeader {
                branch_id: collection_id.branch_id,
                space: collection_id.space.clone(),
                name: collection_id.name.clone(),
                dimension: config.dimension,
                metric: config.metric.to_byte(),
                storage_dtype: config.storage_dtype.to_byte(),
                next_id,
                free_slots,
                count: backend.len() as u32,
                index_type,
                hnsw_graph_state,
            };

            // Write header
            let header_bytes = rmp_serde::to_vec(&header)
                .map_err(|e| VectorError::Serialization(e.to_string()))?;
            writer
                .write_u32::<LittleEndian>(header_bytes.len() as u32)
                .map_err(|e| VectorError::Io(e.to_string()))?;
            writer
                .write_all(&header_bytes)
                .map_err(|e| VectorError::Io(e.to_string()))?;

            // Write vectors in VectorId order (deterministic)
            let vector_ids = backend.vector_ids();
            for vector_id in vector_ids {
                // VectorId
                writer
                    .write_u64::<LittleEndian>(vector_id.as_u64())
                    .map_err(|e| VectorError::Io(e.to_string()))?;

                // Get key and metadata from KV using the collection's
                // actual space — not a hardcoded `"default"`. Without
                // this, snapshotting any collection in `_system_` or
                // a tenant space would fail to fetch its keys.
                let (key, metadata) = self.get_key_and_metadata(
                    collection_id.branch_id,
                    &collection_id.space,
                    &collection_id.name,
                    vector_id,
                )?;

                // Key
                let key_bytes = key.as_bytes();
                writer
                    .write_u32::<LittleEndian>(key_bytes.len() as u32)
                    .map_err(|e| VectorError::Io(e.to_string()))?;
                writer
                    .write_all(key_bytes)
                    .map_err(|e| VectorError::Io(e.to_string()))?;

                // Embedding (raw f32 LE — dequantized if Int8)
                let embedding = backend
                    .get_f32_owned(vector_id)
                    .ok_or_else(|| VectorError::VectorNotFound { key: key.clone() })?;
                for &value in &embedding {
                    writer
                        .write_f32::<LittleEndian>(value)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                }

                // Metadata
                if let Some(ref meta) = metadata {
                    writer
                        .write_u8(1)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                    let meta_bytes = serde_json::to_vec(meta)
                        .map_err(|e| VectorError::Serialization(e.to_string()))?;
                    writer
                        .write_u32::<LittleEndian>(meta_bytes.len() as u32)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                    writer
                        .write_all(&meta_bytes)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                } else {
                    writer
                        .write_u8(0)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    /// Deserialize vector data from snapshot
    ///
    /// This restores:
    /// 1. Collection backends with vectors
    /// 2. next_id and free_slots for each collection (CRITICAL for T4)
    /// 3. VectorRecord metadata in KV
    ///
    /// Maximum header size during snapshot deserialization (1 MB).
    const MAX_SNAPSHOT_HEADER_SIZE: usize = 1_024 * 1_024;
    /// Maximum key size during snapshot deserialization (64 KB).
    const MAX_SNAPSHOT_KEY_SIZE: usize = 64 * 1_024;
    /// Maximum metadata size during snapshot deserialization (64 MB).
    const MAX_SNAPSHOT_METADATA_SIZE: usize = 64 * 1_024 * 1_024;

    /// Deserialize snapshot data from reader.
    pub fn snapshot_deserialize<R: Read>(&self, reader: &mut R) -> VectorResult<()> {
        // Version byte
        let version = reader
            .read_u8()
            .map_err(|e| VectorError::Io(e.to_string()))?;
        if version != VECTOR_SNAPSHOT_VERSION {
            return Err(VectorError::Serialization(format!(
                "Unsupported vector snapshot version: {}",
                version
            )));
        }

        let collection_count = reader
            .read_u32::<LittleEndian>()
            .map_err(|e| VectorError::Io(e.to_string()))?;

        for _ in 0..collection_count {
            // Read header
            let header_len = reader
                .read_u32::<LittleEndian>()
                .map_err(|e| VectorError::Io(e.to_string()))? as usize;
            if header_len > Self::MAX_SNAPSHOT_HEADER_SIZE {
                return Err(VectorError::Serialization(format!(
                    "Snapshot header length {} exceeds maximum {}",
                    header_len,
                    Self::MAX_SNAPSHOT_HEADER_SIZE
                )));
            }
            let mut header_bytes = vec![0u8; header_len];
            reader
                .read_exact(&mut header_bytes)
                .map_err(|e| VectorError::Io(e.to_string()))?;
            let header: CollectionSnapshotHeader = rmp_serde::from_slice(&header_bytes)
                .map_err(|e| VectorError::Serialization(e.to_string()))?;

            // Reconstruct config
            let config = VectorConfig {
                dimension: header.dimension,
                metric: DistanceMetric::from_byte(header.metric).ok_or_else(|| {
                    VectorError::Serialization(format!("Invalid metric: {}", header.metric))
                })?,
                storage_dtype: StorageDtype::F32,
            };

            let collection_id = CollectionId::new(header.branch_id, &header.space, &header.name);

            // Restore collection configuration in KV under the snapshot's
            // recorded space. Snapshot v0x02 added the `space` field; older
            // snapshots are rejected by the version check earlier.
            let collection_record = crate::CollectionRecord::new(&config);
            let config_key = strata_core::types::Key::new_vector_config(
                std::sync::Arc::new(strata_core::types::Namespace::for_branch_space(
                    header.branch_id,
                    &header.space,
                )),
                &header.name,
            );
            let config_bytes = collection_record.to_bytes()?;
            self.db()
                .transaction(header.branch_id, |txn| {
                    txn.put(config_key.clone(), Value::Bytes(config_bytes.clone()))
                })
                .map_err(|e| VectorError::Database(e.to_string()))?;

            // Create backend using factory based on snapshot index_type
            let factory = match header.index_type {
                1 => IndexBackendFactory::Hnsw(crate::hnsw::HnswConfig::default()),
                2 => IndexBackendFactory::SegmentedHnsw(
                    crate::segmented::SegmentedHnswConfig::default(),
                ),
                _ => IndexBackendFactory::default(),
            };
            let mut backend = factory.create(&config);

            // Read and insert vectors
            for _ in 0..header.count {
                // VectorId
                let vector_id = VectorId::new(
                    reader
                        .read_u64::<LittleEndian>()
                        .map_err(|e| VectorError::Io(e.to_string()))?,
                );

                // Key
                let key_len = reader
                    .read_u32::<LittleEndian>()
                    .map_err(|e| VectorError::Io(e.to_string()))?
                    as usize;
                if key_len > Self::MAX_SNAPSHOT_KEY_SIZE {
                    return Err(VectorError::Serialization(format!(
                        "Snapshot key length {} exceeds maximum {}",
                        key_len,
                        Self::MAX_SNAPSHOT_KEY_SIZE
                    )));
                }
                let mut key_bytes = vec![0u8; key_len];
                reader
                    .read_exact(&mut key_bytes)
                    .map_err(|e| VectorError::Io(e.to_string()))?;
                let key = String::from_utf8(key_bytes)
                    .map_err(|e| VectorError::Serialization(e.to_string()))?;

                // Embedding
                let mut embedding = vec![0.0f32; header.dimension];
                for value in &mut embedding {
                    *value = reader
                        .read_f32::<LittleEndian>()
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                }

                // Insert vector into backend
                backend.insert_with_id(vector_id, &embedding)?;

                // Metadata
                let has_metadata = reader
                    .read_u8()
                    .map_err(|e| VectorError::Io(e.to_string()))?
                    != 0;
                let metadata = if has_metadata {
                    let meta_len = reader
                        .read_u32::<LittleEndian>()
                        .map_err(|e| VectorError::Io(e.to_string()))?
                        as usize;
                    if meta_len > Self::MAX_SNAPSHOT_METADATA_SIZE {
                        return Err(VectorError::Serialization(format!(
                            "Snapshot metadata length {} exceeds maximum {}",
                            meta_len,
                            Self::MAX_SNAPSHOT_METADATA_SIZE
                        )));
                    }
                    let mut meta_bytes = vec![0u8; meta_len];
                    reader
                        .read_exact(&mut meta_bytes)
                        .map_err(|e| VectorError::Io(e.to_string()))?;
                    Some(
                        serde_json::from_slice(&meta_bytes)
                            .map_err(|e| VectorError::Serialization(e.to_string()))?,
                    )
                } else {
                    None
                };

                // Store VectorRecord in KV under the snapshot's
                // recorded space (header.space). v0x02 of the
                // snapshot format added `space` to the header so we
                // can restore non-default-space collections correctly.
                let record = VectorRecord::new(vector_id, embedding.clone(), metadata);
                let kv_key =
                    self.vector_key_internal(header.branch_id, &header.space, &header.name, &key);
                let record_bytes = record.to_bytes()?;
                self.db()
                    .transaction(header.branch_id, |txn| {
                        txn.put(kv_key.clone(), Value::Bytes(record_bytes.clone()))
                    })
                    .map_err(|e| VectorError::Database(e.to_string()))?;
            }

            // Restore snapshot state (CRITICAL for T4)
            backend.restore_snapshot_state(header.next_id, header.free_slots);

            // Add backend to store
            self.backends()?.backends.insert(collection_id, backend);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VectorStore;
    use std::io::Cursor;
    use std::sync::Arc;
    use strata_engine::Database;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, VectorStore) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let store = VectorStore::new(db.clone());
        (temp_dir, db, store)
    }

    #[test]
    fn test_snapshot_empty() {
        let (_temp, _db, store) = setup();

        // Serialize empty store
        let mut buffer = Vec::new();
        store.snapshot_serialize(&mut buffer).unwrap();

        // Should have version + collection count (0)
        assert_eq!(buffer.len(), 5); // 1 byte version + 4 bytes count
        assert_eq!(buffer[0], VECTOR_SNAPSHOT_VERSION);

        // Deserialize into new store
        let (_temp2, _db2, store2) = setup();
        let mut cursor = Cursor::new(&buffer);
        store2.snapshot_deserialize(&mut cursor).unwrap();

        // Should have no collections
        let collections = store2.list_collections(BranchId::new(), "default").unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Create collection with vectors
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config.clone())
            .unwrap();

        store
            .insert(branch_id, "default", "test", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "v2",
                &[0.0, 1.0, 0.0],
                Some(serde_json::json!({"type": "doc"})),
            )
            .unwrap();
        store
            .insert(branch_id, "default", "test", "v3", &[0.0, 0.0, 1.0], None)
            .unwrap();

        // Serialize
        let mut buffer = Vec::new();
        store.snapshot_serialize(&mut buffer).unwrap();

        // Deserialize into new store
        let (_temp2, _db2, store2) = setup();
        let mut cursor = Cursor::new(&buffer);
        store2.snapshot_deserialize(&mut cursor).unwrap();

        // Verify collections
        let collections = store2.list_collections(branch_id, "default").unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].name, "test");
        assert_eq!(collections[0].count, 3);

        // Verify vectors
        let v1 = store2
            .get(branch_id, "default", "test", "v1")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(v1.key, "v1");
        assert_eq!(v1.embedding, vec![1.0, 0.0, 0.0]);

        let v2 = store2
            .get(branch_id, "default", "test", "v2")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(v2.metadata, Some(serde_json::json!({"type": "doc"})));
    }

    #[test]
    fn test_snapshot_preserves_next_id() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert and delete to advance next_id
        store
            .insert(branch_id, "default", "test", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "v2", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store.delete(branch_id, "default", "test", "v1").unwrap(); // Delete v1

        // Serialize
        let mut buffer = Vec::new();
        store.snapshot_serialize(&mut buffer).unwrap();

        // Deserialize into new store
        let (_temp2, _db2, store2) = setup();
        let mut cursor = Cursor::new(&buffer);
        store2.snapshot_deserialize(&mut cursor).unwrap();

        // Insert new vector - should get ID >= 2 (not reuse ID 0 or 1)
        store2
            .insert(branch_id, "default", "test", "v3", &[0.0, 0.0, 1.0], None)
            .unwrap();

        // Verify we have v2 and v3 (v1 was deleted)
        assert!(store2
            .get(branch_id, "default", "test", "v1")
            .unwrap()
            .is_none());
        assert!(store2
            .get(branch_id, "default", "test", "v2")
            .unwrap()
            .is_some());
        assert!(store2
            .get(branch_id, "default", "test", "v3")
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_snapshot_multiple_collections() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Create multiple collections
        let config3 = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let config5 = VectorConfig::new(5, DistanceMetric::Euclidean).unwrap();

        store
            .create_collection(branch_id, "default", "col_a", config3)
            .unwrap();
        store
            .create_collection(branch_id, "default", "col_b", config5)
            .unwrap();

        store
            .insert(branch_id, "default", "col_a", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "col_b",
                "v1",
                &[1.0, 0.0, 0.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        // Serialize
        let mut buffer = Vec::new();
        store.snapshot_serialize(&mut buffer).unwrap();

        // Deserialize into new store
        let (_temp2, _db2, store2) = setup();
        let mut cursor = Cursor::new(&buffer);
        store2.snapshot_deserialize(&mut cursor).unwrap();

        // Verify both collections
        let collections = store2.list_collections(branch_id, "default").unwrap();
        assert_eq!(collections.len(), 2);
    }

    #[test]
    fn test_snapshot_deterministic() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(branch_id, "default", "test", "b", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "c", &[0.0, 0.0, 1.0], None)
            .unwrap();

        // Serialize multiple times
        let mut buffer1 = Vec::new();
        store.snapshot_serialize(&mut buffer1).unwrap();

        let mut buffer2 = Vec::new();
        store.snapshot_serialize(&mut buffer2).unwrap();

        // Snapshots should be identical
        assert_eq!(buffer1, buffer2);
    }

    #[test]
    fn test_snapshot_invalid_version() {
        let buffer = vec![0xFF, 0, 0, 0, 0]; // Invalid version
        let (_temp, _db, store) = setup();

        let mut cursor = Cursor::new(&buffer);
        let result = store.snapshot_deserialize(&mut cursor);
        assert!(result.is_err());
    }
}
