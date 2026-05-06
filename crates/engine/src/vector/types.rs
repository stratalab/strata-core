//! Core types for the Vector primitive
//!
//! This module re-exports canonical helper types from the engine-owned surface and defines
//! implementation-specific types for vector storage and search.

// Re-export canonical vector helper types from the engine-owned surface.
pub use crate::semantics::vector::{
    CollectionId, CollectionInfo, DistanceMetric, FilterCondition, FilterOp, JsonScalar,
    MetadataFilter, StorageDtype, VectorConfig, VectorEntry, VectorId, VectorMatch,
};
use crate::vector::VectorError;
use serde_json::Value as JsonValue;

// Re-export EntityRef for source reference linking
pub use strata_core::EntityRef;

// Re-export BranchId for CollectionId usage
pub use strata_core::BranchId;

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

    /// The embedding vector (always stored for history + recovery)
    ///
    /// Stored as Vec<f32> and serialized to bytes via MessagePack.
    /// This enables history() and get_at() to return complete vector snapshots,
    /// and ensures vectors survive recovery without mmap cache.
    /// Must never be empty for newly created records (#1962).
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

    /// Update embedding, metadata and version
    pub fn update(&mut self, embedding: Vec<f32>, metadata: Option<JsonValue>) {
        self.embedding = embedding;
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
    pub fn to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> Result<Self, VectorError> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
    }
}

// ============================================================================
// SearchOptions (Issues #1966, #1967)
// ============================================================================

/// Options for vector search that control filtering, metadata, and over-fetch behavior.
///
/// Use `SearchOptions::default()` for backward-compatible behavior, then chain
/// builder methods to customize:
///
/// ```ignore
/// let opts = SearchOptions::default()
///     .with_filter(filter)
///     .with_include_metadata(true)
///     .with_overfetch_multipliers(vec![2, 4, 8]);
/// store.search_with_options(branch_id, space, "my_collection", &query, 10, opts)?;
/// ```
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Metadata filter (post-filter on search results).
    pub filter: Option<MetadataFilter>,

    /// When true, fetch metadata from KV for each result even when no filter
    /// is active (Issue #1967). Default: `false` (metadata is `None` in
    /// unfiltered results for performance).
    pub include_metadata: bool,

    /// Over-fetch multipliers for filtered search (Issue #1966).
    /// Each round fetches `k * multiplier` candidates from the backend.
    /// Default: `[3, 6, 12]`.
    pub overfetch_multipliers: Vec<usize>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        SearchOptions {
            filter: None,
            include_metadata: false,
            overfetch_multipliers: vec![3, 6, 12],
        }
    }
}

impl SearchOptions {
    /// Set the metadata filter.
    pub fn with_filter(mut self, filter: MetadataFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set whether to include metadata in unfiltered results.
    pub fn with_include_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }

    /// Set custom over-fetch multipliers for filtered search.
    pub fn with_overfetch_multipliers(mut self, multipliers: Vec<usize>) -> Self {
        self.overfetch_multipliers = multipliers;
        self
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

// ============================================================================
// IndexBackendType (Issue #1964)
// ============================================================================

/// Selects which index backend a collection uses.
///
/// Stored as a byte in `CollectionRecord` for persistence.
/// Defaults to `SegmentedHnsw` (the most capable backend).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexBackendType {
    /// Brute-force O(n) scan — lowest memory, best for small collections.
    BruteForce,
    /// Single-segment HNSW graph.
    Hnsw,
    /// Segmented HNSW with auto-sealing — default for all collections.
    #[default]
    SegmentedHnsw,
}

impl IndexBackendType {
    /// Serialize to a single byte for compact storage.
    pub fn to_byte(self) -> u8 {
        match self {
            IndexBackendType::BruteForce => 0,
            IndexBackendType::Hnsw => 1,
            IndexBackendType::SegmentedHnsw => 2,
        }
    }

    /// Deserialize from byte. Returns `None` for unknown values.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(IndexBackendType::BruteForce),
            1 => Some(IndexBackendType::Hnsw),
            2 => Some(IndexBackendType::SegmentedHnsw),
            _ => None,
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

    /// Index backend type (Issue #1964).
    /// Defaults to SegmentedHnsw (byte 2) for backward compatibility
    /// with records that predate this field.
    #[serde(default = "default_backend_type_byte")]
    pub backend_type: u8,
}

fn default_backend_type_byte() -> u8 {
    IndexBackendType::SegmentedHnsw.to_byte()
}

impl CollectionRecord {
    /// Create a new CollectionRecord with the default backend type (SegmentedHnsw).
    pub fn new(config: &VectorConfig) -> Self {
        Self::with_backend_type(config, IndexBackendType::default())
    }

    /// Create a new CollectionRecord with an explicit backend type (Issue #1964).
    pub fn with_backend_type(config: &VectorConfig, backend_type: IndexBackendType) -> Self {
        CollectionRecord {
            config: VectorConfigSerde::from(config),
            created_at: now_micros(),
            backend_type: backend_type.to_byte(),
        }
    }

    /// Get the stored backend type, defaulting to SegmentedHnsw for old records.
    pub fn backend_type(&self) -> IndexBackendType {
        IndexBackendType::from_byte(self.backend_type).unwrap_or(IndexBackendType::SegmentedHnsw)
    }

    /// Serialize to bytes (MessagePack)
    pub fn to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        rmp_serde::to_vec(self).map_err(|e| VectorError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes (MessagePack)
    pub fn from_bytes(data: &[u8]) -> Result<Self, VectorError> {
        rmp_serde::from_slice(data).map_err(|e| VectorError::Serialization(e.to_string()))
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
    type Error = VectorError;

    fn try_from(serde: VectorConfigSerde) -> Result<Self, Self::Error> {
        let metric = DistanceMetric::from_byte(serde.metric).ok_or_else(|| {
            VectorError::Serialization(format!("Invalid metric byte: {}", serde.metric))
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
        use crate::StrataError;
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
        let id = CollectionId::new(branch_id, "default", "my_collection");

        assert_eq!(id.branch_id, branch_id);
        assert_eq!(id.name, "my_collection");
    }

    #[test]
    fn test_collection_id_equality() {
        let branch_id = BranchId::new();
        let id1 = CollectionId::new(branch_id, "default", "collection1");
        let id2 = CollectionId::new(branch_id, "default", "collection1");
        let id3 = CollectionId::new(branch_id, "default", "collection2");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_collection_id_hash() {
        use std::collections::HashSet;
        let branch_id = BranchId::new();

        let mut set = HashSet::new();
        set.insert(CollectionId::new(branch_id, "default", "collection1"));
        set.insert(CollectionId::new(branch_id, "default", "collection2"));
        set.insert(CollectionId::new(branch_id, "default", "collection1")); // Duplicate

        assert_eq!(set.len(), 2);
    }
}
