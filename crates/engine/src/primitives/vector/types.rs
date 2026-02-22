//! Core types for the Vector primitive
//!
//! This module re-exports canonical types from strata-core and defines
//! implementation-specific types for vector storage and search.

use serde_json::Value as JsonValue;

// Re-export canonical vector types from core
pub use strata_core::primitives::{
    CollectionId, CollectionInfo, DistanceMetric, FilterCondition, FilterOp, JsonScalar,
    MetadataFilter, StorageDtype, VectorConfig, VectorEntry, VectorId, VectorMatch,
};

// Re-export EntityRef for source reference linking
pub use strata_core::EntityRef;

// Re-export BranchId for CollectionId usage
pub use strata_core::types::BranchId;

// ============================================================================
// VectorRecord and CollectionRecord (Implementation types)
// ============================================================================

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current time in microseconds since Unix epoch
///
/// Returns 0 if system clock is before Unix epoch (clock went backwards).
pub(crate) fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Lightweight metadata stored inline with VectorId for O(1) search resolution.
/// Eliminates the need for O(n) KV prefix scans during search.
#[derive(Debug, Clone)]
pub struct InlineMeta {
    /// The user-facing key for this vector (collection-relative).
    pub key: String,
    /// Optional source entity reference for hybrid search resolution.
    pub source_ref: Option<EntityRef>,
}

/// Metadata and embedding stored in KV (MessagePack serialized)
///
/// This is stored in the versioned KV storage for:
/// 1. Transaction participation (KV has full tx support)
/// 2. Flexible schema (JSON metadata)
/// 3. WAL integration (reuses existing infrastructure)
/// 4. History support (versioned storage enables history() API)
///
/// The embedding is also stored in VectorHeap for cache-friendly scanning during search.
/// The VectorRecord copy enables history retrieval without modifying the search backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    /// Internal vector ID (maps to VectorHeap)
    pub vector_id: u64,

    /// The embedding vector (stored for history support)
    ///
    /// Stored as Vec<f32> and serialized to bytes via MessagePack.
    /// This enables history() to return complete vector snapshots.
    /// Backwards compatible: old records without this field will deserialize as empty vec.
    #[serde(default)]
    pub embedding: Vec<f32>,

    /// User-provided metadata (optional)
    pub metadata: Option<JsonValue>,

    /// Version for optimistic concurrency
    pub version: u64,

    /// Creation timestamp (microseconds since epoch)
    pub created_at: u64,

    /// Last update timestamp (microseconds since epoch)
    pub updated_at: u64,

    /// Optional reference to source document (e.g., JSON doc, KV entry)
    ///
    /// Used by internal search infrastructure to link embeddings back to
    /// their source documents for hydration during search result assembly.
    /// Backwards compatible: old WAL entries without this field will deserialize as None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<EntityRef>,
}

impl VectorRecord {
    /// Create a new VectorRecord
    pub fn new(vector_id: VectorId, embedding: Vec<f32>, metadata: Option<JsonValue>) -> Self {
        let now = now_micros();
        VectorRecord {
            vector_id: vector_id.as_u64(),
            embedding,
            metadata,
            version: 1,
            created_at: now,
            updated_at: now,
            source_ref: None,
        }
    }

    /// Create a new VectorRecord with a source reference
    ///
    /// Use this when the embedding is derived from another entity (e.g., a JSON document)
    /// and you want to maintain a link back to the source for search result hydration.
    pub fn new_with_source(
        vector_id: VectorId,
        embedding: Vec<f32>,
        metadata: Option<JsonValue>,
        source_ref: EntityRef,
    ) -> Self {
        let now = now_micros();
        VectorRecord {
            vector_id: vector_id.as_u64(),
            embedding,
            metadata,
            version: 1,
            created_at: now,
            updated_at: now,
            source_ref: Some(source_ref),
        }
    }

    /// Create a new VectorRecord without storing the embedding in KV.
    ///
    /// The embedding is stored only in the VectorHeap (and its mmap cache).
    /// This saves ~1.5 KB per vector in KV storage. The `get_at()` fallback
    /// path already handles empty embeddings by reading from the backend.
    pub fn new_lite(vector_id: VectorId, metadata: Option<JsonValue>) -> Self {
        let now = now_micros();
        VectorRecord {
            vector_id: vector_id.as_u64(),
            embedding: Vec::new(),
            metadata,
            version: 1,
            created_at: now,
            updated_at: now,
            source_ref: None,
        }
    }

    /// Create a lite VectorRecord with a source reference (no embedding in KV).
    pub fn new_lite_with_source(
        vector_id: VectorId,
        metadata: Option<JsonValue>,
        source_ref: EntityRef,
    ) -> Self {
        let now = now_micros();
        VectorRecord {
            vector_id: vector_id.as_u64(),
            embedding: Vec::new(),
            metadata,
            version: 1,
            created_at: now,
            updated_at: now,
            source_ref: Some(source_ref),
        }
    }

    /// Update embedding, metadata and version
    pub fn update(&mut self, embedding: Vec<f32>, metadata: Option<JsonValue>) {
        self.embedding = embedding;
        self.metadata = metadata;
        self.version += 1;
        self.updated_at = now_micros();
    }

    /// Update metadata and version without storing the embedding in KV.
    pub fn update_lite(&mut self, metadata: Option<JsonValue>) {
        self.embedding = Vec::new();
        self.metadata = metadata;
        self.version += 1;
        self.updated_at = now_micros();
    }

    /// Update embedding, metadata, source reference, and version
    pub fn update_with_source(
        &mut self,
        embedding: Vec<f32>,
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
    ) {
        self.embedding = embedding;
        self.metadata = metadata;
        self.source_ref = source_ref;
        self.version += 1;
        self.updated_at = now_micros();
    }

    /// Update metadata, source reference, and version without storing embedding in KV.
    pub fn update_lite_with_source(
        &mut self,
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
    ) {
        self.embedding = Vec::new();
        self.metadata = metadata;
        self.source_ref = source_ref;
        self.version += 1;
        self.updated_at = now_micros();
    }

    /// Get the embedding
    pub fn embedding(&self) -> &[f32] {
        &self.embedding
    }

    /// Get VectorId
    pub fn vector_id(&self) -> VectorId {
        VectorId::new(self.vector_id)
    }

    /// Get the source reference, if any
    pub fn source_ref(&self) -> Option<&EntityRef> {
        self.source_ref.as_ref()
    }

    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::primitives::vector::VectorError> {
        rmp_serde::to_vec(self)
            .map_err(|e| crate::primitives::vector::VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::primitives::vector::VectorError> {
        rmp_serde::from_slice(data)
            .map_err(|e| crate::primitives::vector::VectorError::Serialization(e.to_string()))
    }
}

/// Search result with source reference
///
/// Extended version of VectorMatch that includes the source reference.
/// Used by `search_with_sources()` to return results that can be hydrated
/// by looking up the source document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMatchWithSource {
    /// User-provided key
    pub key: String,
    /// Similarity score (higher = more similar)
    pub score: f32,
    /// Optional metadata
    pub metadata: Option<JsonValue>,
    /// Optional reference to source document
    pub source_ref: Option<EntityRef>,
    /// Version of the vector
    pub version: u64,
}

impl VectorMatchWithSource {
    /// Create a new VectorMatchWithSource
    pub fn new(
        key: String,
        score: f32,
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
        version: u64,
    ) -> Self {
        VectorMatchWithSource {
            key,
            score,
            metadata,
            source_ref,
            version,
        }
    }
}

/// Collection configuration stored in KV
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionRecord {
    /// Collection configuration (serializable form)
    pub config: VectorConfigSerde,

    /// Creation timestamp
    pub created_at: u64,
}

impl CollectionRecord {
    /// Create a new CollectionRecord
    pub fn new(config: &VectorConfig) -> Self {
        CollectionRecord {
            config: VectorConfigSerde::from(config),
            created_at: now_micros(),
        }
    }

    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::primitives::vector::VectorError> {
        rmp_serde::to_vec(self)
            .map_err(|e| crate::primitives::vector::VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::primitives::vector::VectorError> {
        rmp_serde::from_slice(data)
            .map_err(|e| crate::primitives::vector::VectorError::Serialization(e.to_string()))
    }
}

/// Serializable version of VectorConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfigSerde {
    /// Embedding dimension
    pub dimension: usize,
    /// Distance metric (as byte)
    pub metric: u8,
    /// Storage data type (as byte)
    pub storage_dtype: u8,
}

impl From<&VectorConfig> for VectorConfigSerde {
    fn from(config: &VectorConfig) -> Self {
        VectorConfigSerde {
            dimension: config.dimension,
            metric: config.metric.to_byte(),
            storage_dtype: config.storage_dtype.to_byte(),
        }
    }
}

impl TryFrom<VectorConfigSerde> for VectorConfig {
    type Error = crate::primitives::vector::VectorError;

    fn try_from(serde: VectorConfigSerde) -> Result<Self, Self::Error> {
        let metric = DistanceMetric::from_byte(serde.metric).ok_or_else(|| {
            crate::primitives::vector::VectorError::Serialization(format!(
                "Invalid metric byte: {}",
                serde.metric
            ))
        })?;

        // Default to F32 for forward compatibility with old WAL entries
        let storage_dtype =
            StorageDtype::from_byte(serde.storage_dtype).unwrap_or(StorageDtype::F32);

        Ok(VectorConfig {
            dimension: serde.dimension,
            metric,
            storage_dtype,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================
    // DistanceMetric Tests (#395)
    // ========================================

    #[test]
    fn test_distance_metric_serialization_roundtrip() {
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            let byte = metric.to_byte();
            let parsed = DistanceMetric::from_byte(byte).unwrap();
            assert_eq!(metric, parsed);
        }
    }

    #[test]
    fn test_distance_metric_from_string() {
        assert_eq!(
            DistanceMetric::parse("cosine"),
            Some(DistanceMetric::Cosine)
        );
        assert_eq!(
            DistanceMetric::parse("COSINE"),
            Some(DistanceMetric::Cosine)
        );
        assert_eq!(
            DistanceMetric::parse("euclidean"),
            Some(DistanceMetric::Euclidean)
        );
        assert_eq!(DistanceMetric::parse("l2"), Some(DistanceMetric::Euclidean));
        assert_eq!(
            DistanceMetric::parse("dot_product"),
            Some(DistanceMetric::DotProduct)
        );
        assert_eq!(
            DistanceMetric::parse("dot"),
            Some(DistanceMetric::DotProduct)
        );
        assert_eq!(
            DistanceMetric::parse("inner_product"),
            Some(DistanceMetric::DotProduct)
        );
        assert_eq!(DistanceMetric::parse("invalid"), None);
    }

    #[test]
    fn test_distance_metric_default() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
    }

    #[test]
    fn test_distance_metric_name() {
        assert_eq!(DistanceMetric::Cosine.name(), "cosine");
        assert_eq!(DistanceMetric::Euclidean.name(), "euclidean");
        assert_eq!(DistanceMetric::DotProduct.name(), "dot_product");
    }

    #[test]
    fn test_distance_metric_from_byte_invalid() {
        assert_eq!(DistanceMetric::from_byte(3), None);
        assert_eq!(DistanceMetric::from_byte(255), None);
    }

    // ========================================
    // VectorConfig Tests (#394)
    // ========================================

    #[test]
    fn test_vector_config_valid() {
        let config = VectorConfig::new(768, DistanceMetric::Cosine).unwrap();
        assert_eq!(config.dimension, 768);
        assert_eq!(config.metric, DistanceMetric::Cosine);
        assert_eq!(config.storage_dtype, StorageDtype::F32);
    }

    #[test]
    fn test_vector_config_zero_dimension() {
        use strata_core::StrataError;
        let result = VectorConfig::new(0, DistanceMetric::Cosine);
        assert!(matches!(result, Err(StrataError::InvalidInput { .. })));
    }

    #[test]
    fn test_preset_configs() {
        assert_eq!(VectorConfig::for_openai_ada().dimension, 1536);
        assert_eq!(VectorConfig::for_openai_large().dimension, 3072);
        assert_eq!(VectorConfig::for_minilm().dimension, 384);
        assert_eq!(VectorConfig::for_mpnet().dimension, 768);
    }

    #[test]
    fn test_vector_config_equality() {
        let config1 = VectorConfig::new(768, DistanceMetric::Cosine).unwrap();
        let config2 = VectorConfig::new(768, DistanceMetric::Cosine).unwrap();
        let config3 = VectorConfig::new(384, DistanceMetric::Cosine).unwrap();

        assert_eq!(config1, config2);
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_storage_dtype_default() {
        assert_eq!(StorageDtype::default(), StorageDtype::F32);
    }

    // ========================================
    // VectorId Tests (#396)
    // ========================================

    #[test]
    fn test_vector_id_ordering() {
        let id1 = VectorId::new(1);
        let id2 = VectorId::new(2);
        let id3 = VectorId::new(1);

        assert!(id1 < id2);
        assert_eq!(id1, id3);

        // BTreeMap ordering for determinism
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert(id2, "second");
        map.insert(id1, "first");

        let keys: Vec<_> = map.keys().collect();
        assert_eq!(keys, vec![&id1, &id2]); // Sorted order
    }

    #[test]
    fn test_vector_id_display() {
        let id = VectorId::new(42);
        assert_eq!(format!("{}", id), "VectorId(42)");
    }

    #[test]
    fn test_vector_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(VectorId::new(1));
        set.insert(VectorId::new(2));
        set.insert(VectorId::new(1)); // Duplicate

        assert_eq!(set.len(), 2);
    }

    // ========================================
    // VectorEntry Tests (#396)
    // ========================================

    #[test]
    fn test_vector_entry_dimension() {
        let entry = VectorEntry::new(
            "test".to_string(),
            vec![1.0, 2.0, 3.0],
            None,
            VectorId::new(1),
        );
        assert_eq!(entry.dimension(), 3);
    }

    #[test]
    fn test_vector_entry_with_metadata() {
        use strata_core::contract::Version;
        let metadata = serde_json::json!({"category": "test"});
        let entry = VectorEntry::new(
            "test".to_string(),
            vec![1.0, 2.0, 3.0],
            Some(metadata.clone()),
            VectorId::new(1),
        );
        assert_eq!(entry.metadata, Some(metadata));
        assert_eq!(entry.version(), Version::txn(1));
    }

    #[test]
    fn test_vector_entry_vector_id() {
        let entry = VectorEntry::new(
            "test".to_string(),
            vec![1.0, 2.0, 3.0],
            None,
            VectorId::new(42),
        );
        assert_eq!(entry.vector_id(), VectorId::new(42));
    }

    // ========================================
    // VectorMatch Tests (#396)
    // ========================================

    #[test]
    fn test_vector_match_creation() {
        let m = VectorMatch::new("key".to_string(), 0.95, None);
        assert_eq!(m.key, "key");
        assert!((m.score - 0.95).abs() < f32::EPSILON);
        assert!(m.metadata.is_none());
    }

    #[test]
    fn test_vector_match_with_metadata() {
        let metadata = serde_json::json!({"source": "doc1"});
        let m = VectorMatch::new("key".to_string(), 0.95, Some(metadata.clone()));
        assert_eq!(m.metadata, Some(metadata));
    }

    // ========================================
    // CollectionInfo Tests (#396)
    // ========================================

    #[test]
    fn test_collection_info() {
        let config = VectorConfig::new(768, DistanceMetric::Cosine).unwrap();
        let info = CollectionInfo {
            name: "test_collection".to_string(),
            config: config.clone(),
            count: 100,
            created_at: 1234567890,
        };

        assert_eq!(info.name, "test_collection");
        assert_eq!(info.config, config);
        assert_eq!(info.count, 100);
        assert_eq!(info.created_at, 1234567890);
    }

    // ========================================
    // CollectionId Tests (#396)
    // ========================================

    #[test]
    fn test_collection_id() {
        let branch_id = BranchId::new();
        let id = CollectionId::new(branch_id, "my_collection");

        assert_eq!(id.branch_id, branch_id);
        assert_eq!(id.name, "my_collection");
    }

    #[test]
    fn test_collection_id_equality() {
        let branch_id = BranchId::new();
        let id1 = CollectionId::new(branch_id, "collection1");
        let id2 = CollectionId::new(branch_id, "collection1");
        let id3 = CollectionId::new(branch_id, "collection2");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_collection_id_hash() {
        use std::collections::HashSet;
        let branch_id = BranchId::new();

        let mut set = HashSet::new();
        set.insert(CollectionId::new(branch_id, "collection1"));
        set.insert(CollectionId::new(branch_id, "collection2"));
        set.insert(CollectionId::new(branch_id, "collection1")); // Duplicate

        assert_eq!(set.len(), 2);
    }
}
