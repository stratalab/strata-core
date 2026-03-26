//! Vector WAL Payloads and Replay
//!
//! This module provides WAL payload types and replay logic for vector operations.
//!
//! ## Design Notes
//!
//! 1. **Transaction-Aware Replay**: WAL replay is handled by the global WAL replayer
//!    which groups entries by transaction ID, only applies committed transactions,
//!    and respects transaction ordering. The vector replayer does NOT need to check
//!    transaction boundaries.
//!
//! 2. **Embedding Storage**: TEMPORARY FORMAT - Full embeddings are stored in WAL
//!    payloads. This bloats WAL size (~3KB per 768-dim vector) but is correct.
//!    future versions may optimize with external embedding storage or delta encoding.
//!
//! 3. **Replay vs Normal Operations**: replay_* methods do NOT write to WAL (they are
//!    replaying from WAL). Normal operations use the VectorStore methods which write WAL.

use crate::{VectorConfig, VectorConfigSerde, VectorError, VectorId, VectorResult};
use serde::{Deserialize, Serialize};
use strata_core::{BranchId, EntityRef};

// ============================================================================
// WAL Entry Type Constants (0x50-0x5F range)
// ============================================================================

/// WAL entry type for vector collection creation
pub const VECTOR_COLLECTION_CREATE: u8 = 0x50;
/// WAL entry type for vector collection deletion
pub const VECTOR_COLLECTION_DELETE: u8 = 0x51;
/// WAL entry type for vector upsert
pub const VECTOR_UPSERT: u8 = 0x52;
/// WAL entry type for vector deletion
pub const VECTOR_DELETE: u8 = 0x53;

// ============================================================================
// WAL Payload Structs
// ============================================================================

/// WAL payload for collection creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalVectorCollectionCreate {
    /// Run ID for this collection
    pub branch_id: BranchId,
    /// Collection name
    pub collection: String,
    /// Collection configuration
    pub config: VectorConfigSerde,
    /// Timestamp of operation (microseconds since epoch)
    pub timestamp: u64,
}

/// WAL payload for collection deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalVectorCollectionDelete {
    /// Run ID for this collection
    pub branch_id: BranchId,
    /// Collection name
    pub collection: String,
    /// Timestamp of operation (microseconds since epoch)
    pub timestamp: u64,
}

/// WAL payload for vector upsert
///
/// WARNING: TEMPORARY FORMAT
/// This payload contains the full embedding, which:
/// - Bloats WAL size significantly (~3KB per 768-dim vector)
/// - Slows down recovery proportionally
///
/// This is acceptable (correctness over performance).
///
/// Future versions may change this to:
/// - Store embeddings in separate segment
/// - Use delta encoding for updates
/// - Reference external embedding storage
///
/// Any such change MUST be versioned and backward compatible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalVectorUpsert {
    /// Run ID for this collection
    pub branch_id: BranchId,
    /// Collection name
    pub collection: String,
    /// User-provided key
    pub key: String,
    /// Internal vector ID
    pub vector_id: u64,
    /// Full embedding (TEMPORARY: Full embedding in WAL)
    pub embedding: Vec<f32>,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
    /// Timestamp of operation (microseconds since epoch)
    pub timestamp: u64,
    /// Optional reference to source document (e.g., JSON doc, KV entry)
    ///
    /// Used by internal search infrastructure to link embeddings back to
    /// their source documents for hydration during search result assembly.
    /// Backwards compatible: old WAL entries without this field will deserialize as None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<EntityRef>,
}

/// WAL payload for vector deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalVectorDelete {
    /// Run ID for this collection
    pub branch_id: BranchId,
    /// Collection name
    pub collection: String,
    /// User-provided key
    pub key: String,
    /// Internal vector ID
    pub vector_id: u64,
    /// Timestamp of operation (microseconds since epoch)
    pub timestamp: u64,
}

// ============================================================================
// Serialization/Deserialization
// ============================================================================

impl WalVectorCollectionCreate {
    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> VectorResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> VectorResult<Self> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
    }
}

impl WalVectorCollectionDelete {
    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> VectorResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> VectorResult<Self> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
    }
}

impl WalVectorUpsert {
    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> VectorResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> VectorResult<Self> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
    }
}

impl WalVectorDelete {
    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> VectorResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> VectorResult<Self> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

// Re-use now_micros from types (single canonical definition).
use crate::types::now_micros;

/// Create a WalVectorCollectionCreate payload
pub fn create_wal_collection_create(
    branch_id: BranchId,
    collection: &str,
    config: &VectorConfig,
) -> WalVectorCollectionCreate {
    WalVectorCollectionCreate {
        branch_id,
        collection: collection.to_string(),
        config: VectorConfigSerde::from(config),
        timestamp: now_micros(),
    }
}

/// Create a WalVectorCollectionDelete payload
pub fn create_wal_collection_delete(
    branch_id: BranchId,
    collection: &str,
) -> WalVectorCollectionDelete {
    WalVectorCollectionDelete {
        branch_id,
        collection: collection.to_string(),
        timestamp: now_micros(),
    }
}

/// Create a WalVectorUpsert payload
pub fn create_wal_upsert(
    branch_id: BranchId,
    collection: &str,
    key: &str,
    vector_id: VectorId,
    embedding: &[f32],
    metadata: Option<serde_json::Value>,
) -> WalVectorUpsert {
    WalVectorUpsert {
        branch_id,
        collection: collection.to_string(),
        key: key.to_string(),
        vector_id: vector_id.as_u64(),
        embedding: embedding.to_vec(),
        metadata,
        timestamp: now_micros(),
        source_ref: None,
    }
}

/// Create a WalVectorUpsert payload with a source reference
///
/// Use this when the embedding is derived from another entity (e.g., a JSON document)
/// and you want to maintain a link back to the source for search result hydration.
pub fn create_wal_upsert_with_source(
    branch_id: BranchId,
    collection: &str,
    key: &str,
    vector_id: VectorId,
    embedding: &[f32],
    metadata: Option<serde_json::Value>,
    source_ref: Option<EntityRef>,
) -> WalVectorUpsert {
    WalVectorUpsert {
        branch_id,
        collection: collection.to_string(),
        key: key.to_string(),
        vector_id: vector_id.as_u64(),
        embedding: embedding.to_vec(),
        metadata,
        timestamp: now_micros(),
        source_ref,
    }
}

/// Create a WalVectorDelete payload
pub fn create_wal_delete(
    branch_id: BranchId,
    collection: &str,
    key: &str,
    vector_id: VectorId,
) -> WalVectorDelete {
    WalVectorDelete {
        branch_id,
        collection: collection.to_string(),
        key: key.to_string(),
        vector_id: vector_id.as_u64(),
        timestamp: now_micros(),
    }
}

// ============================================================================
// VectorWalReplayer
// ============================================================================

use crate::{DistanceMetric, VectorStore};

/// Replayer for Vector WAL entries
///
/// Applies WAL entries to VectorStore during recovery.
/// DOES NOT write new WAL entries (that would cause infinite loops).
pub struct VectorWalReplayer<'a> {
    store: &'a VectorStore,
}

impl<'a> VectorWalReplayer<'a> {
    /// Create a new replayer for the given store
    pub fn new(store: &'a VectorStore) -> Self {
        VectorWalReplayer { store }
    }

    /// Apply a WAL entry
    ///
    /// Deserializes the payload and calls the appropriate replay_* method.
    ///
    /// # Arguments
    ///
    /// * `entry_type` - WAL entry type byte (0x70-0x73 for vector operations)
    /// * `payload` - Serialized WAL payload
    pub fn apply(&self, entry_type: u8, payload: &[u8]) -> VectorResult<()> {
        match entry_type {
            VECTOR_COLLECTION_CREATE => {
                let wal = WalVectorCollectionCreate::from_bytes(payload)?;
                let config = VectorConfig {
                    dimension: wal.config.dimension,
                    metric: DistanceMetric::from_byte(wal.config.metric).ok_or_else(|| {
                        VectorError::Serialization(format!("Invalid metric: {}", wal.config.metric))
                    })?,
                    // Use persisted storage_dtype, default to F32 for backward compatibility
                    storage_dtype: crate::StorageDtype::from_byte(wal.config.storage_dtype)
                        .unwrap_or(crate::StorageDtype::F32),
                };
                self.store
                    .replay_create_collection(wal.branch_id, &wal.collection, config)
            }
            VECTOR_COLLECTION_DELETE => {
                let wal = WalVectorCollectionDelete::from_bytes(payload)?;
                self.store
                    .replay_delete_collection(wal.branch_id, &wal.collection)
            }
            VECTOR_UPSERT => {
                let wal = WalVectorUpsert::from_bytes(payload)?;
                self.store.replay_upsert(
                    wal.branch_id,
                    &wal.collection,
                    &wal.key,
                    VectorId::new(wal.vector_id),
                    &wal.embedding,
                    wal.metadata,
                    wal.source_ref,
                    wal.timestamp,
                )
            }
            VECTOR_DELETE => {
                let wal = WalVectorDelete::from_bytes(payload)?;
                self.store.replay_delete(
                    wal.branch_id,
                    &wal.collection,
                    &wal.key,
                    VectorId::new(wal.vector_id),
                    wal.timestamp,
                )
            }
            _ => {
                // Not a vector entry type - skip silently
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DistanceMetric;

    #[test]
    fn test_collection_create_roundtrip() {
        let branch_id = BranchId::new();
        let config = VectorConfig::new(384, DistanceMetric::Cosine).unwrap();
        let payload = create_wal_collection_create(branch_id, "test_collection", &config);

        // Serialize
        let bytes = payload.to_bytes().unwrap();

        // Deserialize
        let parsed = WalVectorCollectionCreate::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.branch_id, branch_id);
        assert_eq!(parsed.collection, "test_collection");
        assert_eq!(parsed.config.dimension, 384);
        assert_eq!(parsed.config.metric, DistanceMetric::Cosine.to_byte());
    }

    #[test]
    fn test_collection_delete_roundtrip() {
        let branch_id = BranchId::new();
        let payload = create_wal_collection_delete(branch_id, "test_collection");

        let bytes = payload.to_bytes().unwrap();
        let parsed = WalVectorCollectionDelete::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.branch_id, branch_id);
        assert_eq!(parsed.collection, "test_collection");
    }

    #[test]
    fn test_upsert_roundtrip() {
        let branch_id = BranchId::new();
        let vector_id = VectorId::new(42);
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        let metadata = Some(serde_json::json!({"type": "document"}));

        let payload = create_wal_upsert(
            branch_id,
            "test",
            "doc1",
            vector_id,
            &embedding,
            metadata.clone(),
        );

        let bytes = payload.to_bytes().unwrap();
        let parsed = WalVectorUpsert::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.branch_id, branch_id);
        assert_eq!(parsed.collection, "test");
        assert_eq!(parsed.key, "doc1");
        assert_eq!(parsed.vector_id, 42);
        assert_eq!(parsed.embedding, embedding);
        assert_eq!(parsed.metadata, metadata);
    }

    #[test]
    fn test_upsert_without_metadata() {
        let branch_id = BranchId::new();
        let vector_id = VectorId::new(1);
        let embedding = vec![1.0, 2.0, 3.0];

        let payload = create_wal_upsert(branch_id, "col", "key", vector_id, &embedding, None);

        let bytes = payload.to_bytes().unwrap();
        let parsed = WalVectorUpsert::from_bytes(&bytes).unwrap();

        assert!(parsed.metadata.is_none());
    }

    #[test]
    fn test_delete_roundtrip() {
        let branch_id = BranchId::new();
        let vector_id = VectorId::new(99);

        let payload = create_wal_delete(branch_id, "test", "doc1", vector_id);

        let bytes = payload.to_bytes().unwrap();
        let parsed = WalVectorDelete::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.branch_id, branch_id);
        assert_eq!(parsed.collection, "test");
        assert_eq!(parsed.key, "doc1");
        assert_eq!(parsed.vector_id, 99);
    }

    #[test]
    fn test_large_embedding_roundtrip() {
        // Test with a realistic embedding size (768 dimensions)
        let branch_id = BranchId::new();
        let vector_id = VectorId::new(1);
        let embedding: Vec<f32> = (0..768).map(|i| i as f32 * 0.001).collect();

        let payload = create_wal_upsert(branch_id, "test", "doc", vector_id, &embedding, None);

        let bytes = payload.to_bytes().unwrap();

        // Should be roughly 768 * 4 bytes + overhead
        assert!(bytes.len() > 3000);
        assert!(bytes.len() < 4000);

        let parsed = WalVectorUpsert::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.embedding.len(), 768);
        assert!((parsed.embedding[0] - 0.0).abs() < f32::EPSILON);
        assert!((parsed.embedding[767] - 0.767).abs() < 0.001);
    }

    #[test]
    fn test_timestamp_is_set() {
        let branch_id = BranchId::new();
        let payload = create_wal_collection_delete(branch_id, "test");

        // Timestamp should be recent (within last minute)
        let now = now_micros();
        assert!(payload.timestamp <= now);
        assert!(payload.timestamp > now - 60_000_000); // Within 60 seconds
    }

    #[test]
    fn test_invalid_bytes_returns_error() {
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD];

        let result = WalVectorCollectionCreate::from_bytes(&invalid_bytes);
        assert!(result.is_err());

        let result = WalVectorUpsert::from_bytes(&invalid_bytes);
        assert!(result.is_err());
    }
}
