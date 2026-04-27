//! Vector types for the VectorStore primitive
//!
//! These types define the structure of vector embeddings and search results.
//! Implementation logic (distance calculations, indexing, ANN) remains in primitives.

use crate::contract::{EntityRef, Version};
use crate::error::StrataError;
use crate::types::BranchId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Distance metric for similarity calculation
///
/// All metrics are normalized to "higher = more similar".
/// This normalization is part of the interface contract.
///
/// Note: The actual distance calculation logic is in the primitives crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity: dot(a,b) / (||a|| * ||b||)
    /// Range: [-1, 1], higher = more similar
    /// Best for: normalized embeddings, semantic similarity
    #[default]
    Cosine,

    /// Euclidean similarity: 1 / (1 + l2_distance)
    /// Range: (0, 1], higher = more similar
    /// Best for: absolute position comparisons
    Euclidean,

    /// Dot product (raw value)
    /// Range: unbounded, higher = more similar
    /// Best for: pre-normalized embeddings, retrieval
    /// WARNING: Assumes vectors are normalized. Non-normalized vectors
    /// will produce unbounded scores.
    DotProduct,
}

impl DistanceMetric {
    /// Human-readable name for display
    pub fn name(&self) -> &'static str {
        match self {
            DistanceMetric::Cosine => "cosine",
            DistanceMetric::Euclidean => "euclidean",
            DistanceMetric::DotProduct => "dot_product",
        }
    }

    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cosine" => Some(DistanceMetric::Cosine),
            "euclidean" | "l2" => Some(DistanceMetric::Euclidean),
            "dot_product" | "dot" | "inner_product" => Some(DistanceMetric::DotProduct),
            _ => None,
        }
    }

    /// Serialization value for WAL/snapshot
    pub fn to_byte(&self) -> u8 {
        match self {
            DistanceMetric::Cosine => 0,
            DistanceMetric::Euclidean => 1,
            DistanceMetric::DotProduct => 2,
        }
    }

    /// Deserialization from WAL/snapshot
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(DistanceMetric::Cosine),
            1 => Some(DistanceMetric::Euclidean),
            2 => Some(DistanceMetric::DotProduct),
            _ => None,
        }
    }
}

/// Storage data type for embeddings
///
/// Controls how embeddings are stored in the VectorHeap.
/// Int8 scalar quantization provides 4x memory savings with ~1-2% recall loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StorageDtype {
    /// 32-bit floating point (default)
    #[default]
    F32,
    // F16,     // Reserved for half precision (value = 1)
    /// 8-bit unsigned integer via scalar quantization (SQ8)
    ///
    /// Embeddings are quantized per-dimension using min/max calibration.
    /// Queries remain f32 (asymmetric distance computation).
    /// 4x memory savings vs F32 with ~1-2% recall loss.
    Int8,
    /// Binary quantization via RaBitQ (SIGMOD 2024).
    ///
    /// D-dimensional vectors → D bits using random orthogonal rotation + sign encoding.
    /// 32x compression from F32 with ~5% recall loss.
    Binary,
}

impl StorageDtype {
    /// Serialization value for WAL/snapshot
    pub fn to_byte(&self) -> u8 {
        match self {
            StorageDtype::F32 => 0,
            StorageDtype::Int8 => 2,
            StorageDtype::Binary => 3,
        }
    }

    /// Deserialization from WAL/snapshot
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(StorageDtype::F32),
            2 => Some(StorageDtype::Int8),
            3 => Some(StorageDtype::Binary),
            _ => None,
        }
    }

    /// Bytes per element for this storage type
    pub fn element_size(&self) -> usize {
        match self {
            StorageDtype::F32 => 4,
            StorageDtype::Int8 => 1,
            StorageDtype::Binary => 1, // notional; binary uses ceil(dim/8) per vector
        }
    }
}

/// Collection configuration - immutable after creation
///
/// IMPORTANT: This struct must NOT contain backend-specific fields.
/// HNSW parameters (ef_construction, M, etc.) belong in backend config, not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Embedding dimension (e.g., 384, 768, 1536)
    /// Must be > 0. Immutable after collection creation.
    pub dimension: usize,

    /// Distance metric for similarity calculation
    /// Immutable after collection creation.
    pub metric: DistanceMetric,

    /// Storage data type (F32 or Int8 scalar quantization)
    pub storage_dtype: StorageDtype,
}

impl VectorConfig {
    /// Create a new VectorConfig with validation
    ///
    /// Returns an error if dimension is 0.
    pub fn new(dimension: usize, metric: DistanceMetric) -> Result<Self, StrataError> {
        if dimension == 0 {
            return Err(StrataError::InvalidInput {
                message: format!("Invalid dimension: {} (must be > 0)", dimension),
            });
        }
        Ok(VectorConfig {
            dimension,
            metric,
            storage_dtype: StorageDtype::F32,
        })
    }

    /// Create a new VectorConfig with explicit storage dtype
    pub fn new_with_dtype(
        dimension: usize,
        metric: DistanceMetric,
        storage_dtype: StorageDtype,
    ) -> Result<Self, StrataError> {
        if dimension == 0 {
            return Err(StrataError::InvalidInput {
                message: format!("Invalid dimension: {} (must be > 0)", dimension),
            });
        }
        Ok(VectorConfig {
            dimension,
            metric,
            storage_dtype,
        })
    }

    /// Config for OpenAI text-embedding-ada-002 (1536 dims)
    pub fn for_openai_ada() -> Self {
        VectorConfig {
            dimension: 1536,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        }
    }

    /// Config for OpenAI text-embedding-3-large (3072 dims)
    pub fn for_openai_large() -> Self {
        VectorConfig {
            dimension: 3072,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        }
    }

    /// Config for MiniLM (384 dims)
    pub fn for_minilm() -> Self {
        VectorConfig {
            dimension: 384,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        }
    }

    /// Config for an embedding model with the given dimension.
    pub fn for_embedding(dimension: usize) -> Self {
        VectorConfig {
            dimension,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        }
    }

    /// Config for sentence-transformers/all-mpnet-base-v2 (768 dims)
    pub fn for_mpnet() -> Self {
        VectorConfig {
            dimension: 768,
            metric: DistanceMetric::Cosine,
            storage_dtype: StorageDtype::F32,
        }
    }
}

/// Internal vector identifier (stable within collection)
///
/// IMPORTANT: VectorIds are never reused.
/// Storage slots may be reused, but the ID value is monotonically increasing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VectorId(pub u64);

impl VectorId {
    /// Create a new VectorId
    pub fn new(id: u64) -> Self {
        VectorId(id)
    }

    /// Get the underlying u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for VectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VectorId({})", self.0)
    }
}

/// Vector entry stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    /// User-provided key (unique within collection)
    pub key: String,

    /// Embedding vector
    pub embedding: Vec<f32>,

    /// Optional JSON metadata
    pub metadata: Option<serde_json::Value>,

    /// Internal ID (for index backend)
    pub vector_id: VectorId,

    /// Version for optimistic concurrency (Txn-based)
    pub version: Version,

    /// Optional reference to source document (e.g., JSON doc, KV entry)
    ///
    /// Used by internal search infrastructure to link embeddings back to
    /// their source documents for hydration during search result assembly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<EntityRef>,
}

impl VectorEntry {
    /// Create a new VectorEntry
    pub fn new(
        key: String,
        embedding: Vec<f32>,
        metadata: Option<serde_json::Value>,
        vector_id: VectorId,
    ) -> Self {
        VectorEntry {
            key,
            embedding,
            metadata,
            vector_id,
            version: Version::txn(1),
            source_ref: None,
        }
    }

    /// Create a new VectorEntry with a source reference
    ///
    /// Use this when the embedding is derived from another entity (e.g., a JSON document)
    /// and you want to maintain a link back to the source for search result hydration.
    pub fn new_with_source(
        key: String,
        embedding: Vec<f32>,
        metadata: Option<serde_json::Value>,
        vector_id: VectorId,
        source_ref: EntityRef,
    ) -> Self {
        VectorEntry {
            key,
            embedding,
            metadata,
            vector_id,
            version: Version::txn(1),
            source_ref: Some(source_ref),
        }
    }

    /// Get the embedding dimension
    pub fn dimension(&self) -> usize {
        self.embedding.len()
    }

    /// Get the version
    pub fn version(&self) -> Version {
        self.version
    }

    /// Get the vector ID
    pub fn vector_id(&self) -> VectorId {
        self.vector_id
    }

    /// Get the source reference, if any
    pub fn source_ref(&self) -> Option<&EntityRef> {
        self.source_ref.as_ref()
    }
}

/// Search result entry
///
/// Returned by search operations. Score is always "higher = more similar"
/// regardless of the underlying distance metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMatch {
    /// User-provided key
    pub key: String,

    /// Similarity score (higher = more similar)
    /// This is normalized per the interface contract.
    pub score: f32,

    /// Optional metadata (if requested and present)
    pub metadata: Option<serde_json::Value>,
}

impl VectorMatch {
    /// Create a new VectorMatch
    pub fn new(key: String, score: f32, metadata: Option<serde_json::Value>) -> Self {
        VectorMatch {
            key,
            score,
            metadata,
        }
    }
}

/// Collection metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Collection name
    pub name: String,

    /// Immutable configuration
    pub config: VectorConfig,

    /// Current vector count
    pub count: usize,

    /// Creation timestamp (microseconds since epoch)
    pub created_at: u64,
}

/// Unique identifier for a collection within a branch.
///
/// `(branch_id, space, name)` is the real storage-key tuple for a vector
/// collection: primary KV uses `Namespace::for_branch_space(branch_id, space)`
/// as its prefix and the collection name as the segment under that. Two
/// collections with the same `(branch_id, name)` in different spaces are
/// completely independent — they have distinct backends, distinct indexes,
/// distinct counts, and distinct on-disk caches.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionId {
    /// Branch ID this collection belongs to
    pub branch_id: BranchId,
    /// Space within the branch (e.g. `"default"`, `"tenant_a"`, `"_system_"`)
    pub space: String,
    /// Collection name
    pub name: String,
}

impl CollectionId {
    /// Create a new CollectionId
    pub fn new(branch_id: BranchId, space: impl Into<String>, name: impl Into<String>) -> Self {
        CollectionId {
            branch_id,
            space: space.into(),
            name: name.into(),
        }
    }
}

impl Ord for CollectionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.branch_id
            .as_bytes()
            .cmp(other.branch_id.as_bytes())
            .then(self.space.cmp(&other.space))
            .then(self.name.cmp(&other.name))
    }
}

impl PartialOrd for CollectionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// JSON scalar value for filtering
///
/// Only scalar values can be used in equality filters.
/// Complex types (arrays, objects) are not supported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JsonScalar {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Numeric value (stored as f64)
    Number(f64),
    /// String value
    String(String),
}

impl JsonScalar {
    /// Check if this scalar matches a JSON value
    pub fn matches_json(&self, value: &serde_json::Value) -> bool {
        match (self, value) {
            (JsonScalar::Null, serde_json::Value::Null) => true,
            (JsonScalar::Bool(a), serde_json::Value::Bool(b)) => a == b,
            (JsonScalar::Number(a), serde_json::Value::Number(b)) => {
                b.as_f64().is_some_and(|n| (a - n).abs() < f64::EPSILON)
            }
            (JsonScalar::String(a), serde_json::Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl From<bool> for JsonScalar {
    fn from(v: bool) -> Self {
        JsonScalar::Bool(v)
    }
}

impl From<i32> for JsonScalar {
    fn from(v: i32) -> Self {
        JsonScalar::Number(v as f64)
    }
}

impl From<i64> for JsonScalar {
    fn from(v: i64) -> Self {
        JsonScalar::Number(v as f64)
    }
}

impl From<f32> for JsonScalar {
    fn from(v: f32) -> Self {
        JsonScalar::Number(v as f64)
    }
}

impl From<f64> for JsonScalar {
    fn from(v: f64) -> Self {
        JsonScalar::Number(v)
    }
}

impl From<String> for JsonScalar {
    fn from(v: String) -> Self {
        JsonScalar::String(v)
    }
}

impl From<&str> for JsonScalar {
    fn from(v: &str) -> Self {
        JsonScalar::String(v.to_string())
    }
}

/// Filter operation for metadata conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Greater than (numeric only)
    Gt,
    /// Greater than or equal (numeric only)
    Gte,
    /// Less than (numeric only)
    Lt,
    /// Less than or equal (numeric only)
    Lte,
    /// Value is one of a list (value must be an array of scalars)
    In,
    /// String contains substring (string only)
    Contains,
}

/// A single filter condition on a metadata field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterCondition {
    /// Metadata field name
    pub field: String,
    /// Filter operation
    pub op: FilterOp,
    /// Value to compare against
    pub value: JsonScalar,
}

/// Metadata filter for search
///
/// Supports equality filtering via `equals` (backwards-compatible) and
/// advanced filtering via `conditions` (Ne, Gt, Gte, Lt, Lte, In, Contains).
/// All conditions use AND semantics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// Top-level field equality (scalar values only)
    /// All conditions must match (AND semantics)
    /// Kept for backwards compatibility.
    pub equals: HashMap<String, JsonScalar>,
    /// Advanced filter conditions (AND semantics)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<FilterCondition>,
}

impl MetadataFilter {
    /// Create an empty filter (matches all)
    pub fn new() -> Self {
        MetadataFilter {
            equals: HashMap::new(),
            conditions: Vec::new(),
        }
    }

    /// Add an equality condition (legacy builder)
    pub fn eq(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.equals.insert(field.into(), value.into());
        self
    }

    /// Add a not-equal condition
    pub fn ne(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Ne,
            value: value.into(),
        });
        self
    }

    /// Add a greater-than condition (numeric only)
    pub fn gt(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Gt,
            value: value.into(),
        });
        self
    }

    /// Add a greater-than-or-equal condition (numeric only)
    pub fn gte(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Gte,
            value: value.into(),
        });
        self
    }

    /// Add a less-than condition (numeric only)
    pub fn lt(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Lt,
            value: value.into(),
        });
        self
    }

    /// Add a less-than-or-equal condition (numeric only)
    pub fn lte(mut self, field: impl Into<String>, value: impl Into<JsonScalar>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Lte,
            value: value.into(),
        });
        self
    }

    /// Add an "in" condition (value must match one of the provided scalars)
    pub fn in_values(mut self, field: impl Into<String>, values: Vec<JsonScalar>) -> Self {
        let field_name: String = field.into();
        for val in values {
            self.conditions.push(FilterCondition {
                field: field_name.clone(),
                op: FilterOp::In,
                value: val,
            });
        }
        self
    }

    /// Add a string-contains condition (string only)
    pub fn contains(mut self, field: impl Into<String>, substring: impl Into<String>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            op: FilterOp::Contains,
            value: JsonScalar::String(substring.into()),
        });
        self
    }

    /// Check if metadata matches this filter
    ///
    /// Returns true if all conditions match (AND semantics).
    /// Returns false if metadata is None and filter is non-empty.
    pub fn matches(&self, metadata: &Option<serde_json::Value>) -> bool {
        if self.equals.is_empty() && self.conditions.is_empty() {
            return true;
        }

        let Some(meta) = metadata else {
            return false;
        };

        let Some(obj) = meta.as_object() else {
            return false;
        };

        // Check legacy equality conditions
        for (key, expected) in &self.equals {
            let Some(actual) = obj.get(key) else {
                return false;
            };
            if !expected.matches_json(actual) {
                return false;
            }
        }

        // Check advanced conditions
        // For "In" ops, group by field: at least one In value must match
        let mut in_groups: HashMap<String, Vec<&JsonScalar>> = HashMap::new();
        for cond in &self.conditions {
            if cond.op == FilterOp::In {
                in_groups
                    .entry(cond.field.clone())
                    .or_default()
                    .push(&cond.value);
                continue;
            }

            let Some(actual) = obj.get(&cond.field) else {
                return false;
            };
            if !eval_condition(&cond.op, &cond.value, actual) {
                return false;
            }
        }

        // Evaluate In groups: field value must match at least one value in the group
        for (field, values) in &in_groups {
            let Some(actual) = obj.get(field) else {
                return false;
            };
            let any_match = values.iter().any(|v| v.matches_json(actual));
            if !any_match {
                return false;
            }
        }

        true
    }

    /// Check if filter is empty (matches all)
    pub fn is_empty(&self) -> bool {
        self.equals.is_empty() && self.conditions.is_empty()
    }

    /// Get the number of conditions in the filter
    pub fn len(&self) -> usize {
        self.equals.len() + self.conditions.len()
    }
}

/// Evaluate a single filter condition against a JSON value
fn eval_condition(op: &FilterOp, expected: &JsonScalar, actual: &serde_json::Value) -> bool {
    match op {
        FilterOp::Eq => expected.matches_json(actual),
        FilterOp::Ne => !expected.matches_json(actual),
        FilterOp::Gt => {
            numeric_cmp(expected, actual).is_some_and(|ord| ord == std::cmp::Ordering::Less)
        }
        FilterOp::Gte => {
            numeric_cmp(expected, actual).is_some_and(|ord| ord != std::cmp::Ordering::Greater)
        }
        FilterOp::Lt => {
            numeric_cmp(expected, actual).is_some_and(|ord| ord == std::cmp::Ordering::Greater)
        }
        FilterOp::Lte => {
            numeric_cmp(expected, actual).is_some_and(|ord| ord != std::cmp::Ordering::Less)
        }
        FilterOp::In => {
            // Single value check (grouped evaluation is done in matches())
            expected.matches_json(actual)
        }
        FilterOp::Contains => {
            // String contains substring
            match (expected, actual) {
                (JsonScalar::String(substring), serde_json::Value::String(s)) => {
                    s.contains(substring.as_str())
                }
                _ => false,
            }
        }
    }
}

/// Compare expected scalar against actual JSON value numerically.
/// Returns the ordering of expected relative to actual (expected.cmp(actual)).
fn numeric_cmp(expected: &JsonScalar, actual: &serde_json::Value) -> Option<std::cmp::Ordering> {
    let expected_num = match expected {
        JsonScalar::Number(n) => *n,
        _ => return None,
    };
    let actual_num = actual.as_f64()?;
    expected_num.partial_cmp(&actual_num)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ================================================================
    // VectorId
    // ================================================================

    #[test]
    fn test_vector_id_creation() {
        let id = VectorId::new(42);
        assert_eq!(id.as_u64(), 42);
    }

    #[test]
    fn test_vector_id_ordering() {
        let a = VectorId::new(1);
        let b = VectorId::new(2);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(VectorId::new(5), VectorId::new(5));
    }

    #[test]
    fn test_vector_id_zero() {
        let id = VectorId::new(0);
        assert_eq!(id.as_u64(), 0);
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
        set.insert(VectorId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    // ================================================================
    // DistanceMetric
    // ================================================================

    #[test]
    fn test_distance_metric_variants() {
        assert_eq!(DistanceMetric::Cosine.name(), "cosine");
        assert_eq!(DistanceMetric::Euclidean.name(), "euclidean");
        assert_eq!(DistanceMetric::DotProduct.name(), "dot_product");
    }

    #[test]
    fn test_distance_metric_default() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
    }

    #[test]
    fn test_distance_metric_parse() {
        assert_eq!(
            DistanceMetric::parse("cosine"),
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
        assert_eq!(
            DistanceMetric::parse("COSINE"),
            Some(DistanceMetric::Cosine)
        ); // case-insensitive
        assert_eq!(DistanceMetric::parse("unknown"), None);
    }

    #[test]
    fn test_distance_metric_byte_roundtrip() {
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            let byte = metric.to_byte();
            let restored = DistanceMetric::from_byte(byte).unwrap();
            assert_eq!(metric, restored);
        }
        assert!(DistanceMetric::from_byte(255).is_none());
    }

    // ================================================================
    // StorageDtype
    // ================================================================

    #[test]
    fn test_storage_dtype_default() {
        assert_eq!(StorageDtype::default(), StorageDtype::F32);
    }

    #[test]
    fn test_storage_dtype_byte_roundtrip() {
        let dtype = StorageDtype::F32;
        let byte = dtype.to_byte();
        let restored = StorageDtype::from_byte(byte).unwrap();
        assert_eq!(dtype, restored);
        assert!(StorageDtype::from_byte(255).is_none());
    }

    // ================================================================
    // VectorConfig
    // ================================================================

    #[test]
    fn test_vector_config_new() {
        let config = VectorConfig::new(384, DistanceMetric::Cosine).unwrap();
        assert_eq!(config.dimension, 384);
        assert_eq!(config.metric, DistanceMetric::Cosine);
        assert_eq!(config.storage_dtype, StorageDtype::F32);
    }

    #[test]
    fn test_vector_config_zero_dimension_rejected() {
        let result = VectorConfig::new(0, DistanceMetric::Cosine);
        assert!(result.is_err());
    }

    #[test]
    fn test_vector_config_presets() {
        let ada = VectorConfig::for_openai_ada();
        assert_eq!(ada.dimension, 1536);

        let large = VectorConfig::for_openai_large();
        assert_eq!(large.dimension, 3072);

        let minilm = VectorConfig::for_minilm();
        assert_eq!(minilm.dimension, 384);

        let mpnet = VectorConfig::for_mpnet();
        assert_eq!(mpnet.dimension, 768);
    }

    #[test]
    fn test_vector_config_serialization() {
        let config = VectorConfig::new(384, DistanceMetric::Euclidean).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let restored: VectorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, restored);
    }

    // ================================================================
    // VectorEntry
    // ================================================================

    #[test]
    fn test_vector_entry_new() {
        let entry = VectorEntry::new(
            "doc-1".to_string(),
            vec![1.0, 2.0, 3.0],
            None,
            VectorId::new(1),
        );
        assert_eq!(entry.key, "doc-1");
        assert_eq!(entry.dimension(), 3);
        assert_eq!(entry.vector_id(), VectorId::new(1));
        assert_eq!(entry.version(), Version::txn(1));
        assert!(entry.metadata.is_none());
        assert!(entry.source_ref().is_none());
    }

    #[test]
    fn test_vector_entry_with_metadata() {
        let meta = serde_json::json!({"category": "science"});
        let entry = VectorEntry::new(
            "doc-2".to_string(),
            vec![0.5; 384],
            Some(meta.clone()),
            VectorId::new(2),
        );
        assert_eq!(entry.metadata, Some(meta));
    }

    #[test]
    fn test_vector_entry_with_source_ref() {
        let branch_id = BranchId::new();
        let source = EntityRef::json(branch_id, "default", "source-doc");
        let entry = VectorEntry::new_with_source(
            "emb-1".to_string(),
            vec![1.0, 2.0],
            None,
            VectorId::new(3),
            source.clone(),
        );
        assert_eq!(entry.source_ref(), Some(&source));
    }

    // ================================================================
    // VectorMatch
    // ================================================================

    #[test]
    fn test_vector_match_new() {
        let m = VectorMatch::new("key-1".to_string(), 0.95, None);
        assert_eq!(m.key, "key-1");
        assert!((m.score - 0.95).abs() < f32::EPSILON);
        assert!(m.metadata.is_none());
    }

    // ================================================================
    // CollectionId
    // ================================================================

    #[test]
    fn test_collection_id_equality() {
        let branch_id = BranchId::new();
        let c1 = CollectionId::new(branch_id, "default", "embeddings");
        let c2 = CollectionId::new(branch_id, "default", "embeddings");
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_collection_id_ordering() {
        let r1 = BranchId::new();
        let r2 = BranchId::new();
        let c1 = CollectionId::new(r1, "default", "a");
        let c2 = CollectionId::new(r1, "default", "b");
        // Same branch_id, different name: should have defined ordering
        // CollectionId has a defined total ordering
        let _has_ordering = c1.cmp(&c2);
        // Different branch_id
        let c3 = CollectionId::new(r2, "default", "a");
        assert_ne!(c1, c3);
    }

    /// Two collections with the same `(branch, name)` in different
    /// spaces must be distinct values, distinct hashes, and distinct
    /// in any `Ord`-based collection. This is the core invariant of
    /// the Phase 0 `CollectionId` change — without it, vector backend
    /// state collides across tenants.
    #[test]
    fn test_collection_id_space_distinguishes_identity() {
        use std::collections::hash_map::DefaultHasher;
        use std::collections::BTreeSet;
        use std::hash::{Hash, Hasher};

        fn hash_of(c: &CollectionId) -> u64 {
            let mut h = DefaultHasher::new();
            c.hash(&mut h);
            h.finish()
        }

        let bid = BranchId::new();
        let a = CollectionId::new(bid, "tenant_a", "embeddings");
        let b = CollectionId::new(bid, "tenant_b", "embeddings");

        assert_ne!(a, b);
        assert_ne!(hash_of(&a), hash_of(&b));

        let mut set = BTreeSet::new();
        set.insert(a.clone());
        set.insert(b.clone());
        assert_eq!(set.len(), 2);
    }

    // ================================================================
    // JsonScalar
    // ================================================================

    #[test]
    fn test_json_scalar_from_conversions() {
        let s: JsonScalar = true.into();
        assert_eq!(s, JsonScalar::Bool(true));

        let s: JsonScalar = 42i32.into();
        assert!(matches!(s, JsonScalar::Number(_)));

        let s: JsonScalar = 2.78f64.into();
        assert!(matches!(s, JsonScalar::Number(_)));

        let s: JsonScalar = "hello".into();
        assert_eq!(s, JsonScalar::String("hello".to_string()));
    }

    #[test]
    fn test_json_scalar_matches_json() {
        assert!(JsonScalar::Null.matches_json(&serde_json::Value::Null));
        assert!(JsonScalar::Bool(true).matches_json(&serde_json::json!(true)));
        assert!(JsonScalar::Number(42.0).matches_json(&serde_json::json!(42)));
        assert!(JsonScalar::String("hi".to_string()).matches_json(&serde_json::json!("hi")));
        // Mismatched types
        assert!(!JsonScalar::Bool(true).matches_json(&serde_json::json!(1)));
        assert!(!JsonScalar::Null.matches_json(&serde_json::json!(false)));
    }

    // ================================================================
    // MetadataFilter
    // ================================================================

    #[test]
    fn test_metadata_filter_empty_matches_all() {
        let filter = MetadataFilter::new();
        assert!(filter.is_empty());
        assert!(filter.matches(&None));
        assert!(filter.matches(&Some(serde_json::json!({"any": "value"}))));
    }

    #[test]
    fn test_metadata_filter_eq_match() {
        let filter = MetadataFilter::new().eq("color", "red").eq("active", true);

        assert_eq!(filter.len(), 2);

        let meta = Some(serde_json::json!({"color": "red", "active": true}));
        assert!(filter.matches(&meta));

        let meta_wrong = Some(serde_json::json!({"color": "blue", "active": true}));
        assert!(!filter.matches(&meta_wrong));
    }

    #[test]
    fn test_metadata_filter_none_metadata_no_match() {
        let filter = MetadataFilter::new().eq("key", "val");
        assert!(!filter.matches(&None));
    }

    #[test]
    fn test_metadata_filter_non_object_no_match() {
        let filter = MetadataFilter::new().eq("key", "val");
        assert!(!filter.matches(&Some(serde_json::json!("not an object"))));
    }

    #[test]
    fn test_metadata_filter_missing_field_no_match() {
        let filter = MetadataFilter::new().eq("missing", "val");
        let meta = Some(serde_json::json!({"other": "val"}));
        assert!(!filter.matches(&meta));
    }

    #[test]
    fn test_metadata_filter_extra_fields_still_match() {
        // Filter only checks specified fields - extra fields in metadata are ignored
        let filter = MetadataFilter::new().eq("color", "red");
        let meta = Some(serde_json::json!({"color": "red", "size": 42, "extra": true}));
        assert!(filter.matches(&meta));
    }

    #[test]
    fn test_metadata_filter_array_metadata_no_match() {
        let filter = MetadataFilter::new().eq("key", "val");
        assert!(!filter.matches(&Some(serde_json::json!([1, 2, 3]))));
    }

    // ================================================================
    // CollectionInfo
    // ================================================================

    #[test]
    fn test_collection_info_serialization_roundtrip() {
        let info = CollectionInfo {
            name: "embeddings".to_string(),
            config: VectorConfig::for_minilm(),
            count: 1000,
            created_at: 1_700_000_000_000_000,
        };
        let json = serde_json::to_string(&info).unwrap();
        let restored: CollectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.name, "embeddings");
        assert_eq!(restored.config.dimension, 384);
        assert_eq!(restored.count, 1000);
        assert_eq!(restored.created_at, 1_700_000_000_000_000);
    }

    #[test]
    fn test_collection_info_zero_count() {
        let info = CollectionInfo {
            name: "empty".to_string(),
            config: VectorConfig::new(128, DistanceMetric::Euclidean).unwrap(),
            count: 0,
            created_at: 0,
        };
        assert_eq!(info.count, 0);
        assert_eq!(info.created_at, 0);
    }

    // ================================================================
    // VectorEntry serialization & edge cases
    // ================================================================

    #[test]
    fn test_vector_entry_serialization_roundtrip() {
        let entry = VectorEntry::new(
            "doc-1".to_string(),
            vec![1.0, 2.0, 3.0],
            Some(serde_json::json!({"k": "v"})),
            VectorId::new(7),
        );
        let json = serde_json::to_string(&entry).unwrap();
        let restored: VectorEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.key, "doc-1");
        assert_eq!(restored.embedding, vec![1.0, 2.0, 3.0]);
        assert_eq!(restored.vector_id, VectorId::new(7));
        assert_eq!(restored.metadata, Some(serde_json::json!({"k": "v"})));
    }

    #[test]
    fn test_vector_entry_source_ref_skip_serializing_if_none() {
        // source_ref should be absent from JSON when None
        let entry = VectorEntry::new("k".to_string(), vec![1.0], None, VectorId::new(1));
        let json = serde_json::to_string(&entry).unwrap();
        assert!(
            !json.contains("source_ref"),
            "source_ref should be skipped when None"
        );

        // Deserializing JSON without source_ref should produce None
        let restored: VectorEntry = serde_json::from_str(&json).unwrap();
        assert!(restored.source_ref.is_none());
    }

    #[test]
    fn test_vector_entry_source_ref_serialization_roundtrip() {
        let branch_id = BranchId::new();
        let source = EntityRef::json(branch_id, "default", "source-doc");
        let entry = VectorEntry::new_with_source(
            "emb".to_string(),
            vec![1.0],
            None,
            VectorId::new(1),
            source.clone(),
        );
        let json = serde_json::to_string(&entry).unwrap();
        assert!(
            json.contains("source_ref"),
            "source_ref should be present when Some"
        );
        let restored: VectorEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.source_ref, Some(source));
    }

    #[test]
    fn test_vector_entry_empty_embedding() {
        let entry = VectorEntry::new("empty".to_string(), vec![], None, VectorId::new(0));
        assert_eq!(entry.dimension(), 0);
    }

    #[test]
    fn test_vector_entry_nan_in_embedding() {
        // NaN is a valid f32 and can appear in embeddings
        let entry = VectorEntry::new(
            "nan".to_string(),
            vec![f32::NAN, 1.0, f32::NEG_INFINITY],
            None,
            VectorId::new(1),
        );
        assert!(entry.embedding[0].is_nan());
        assert!(entry.embedding[2].is_infinite());
        assert_eq!(entry.dimension(), 3);
    }

    #[test]
    fn test_vector_entry_large_dimension() {
        let entry = VectorEntry::new("large".to_string(), vec![0.0; 4096], None, VectorId::new(1));
        assert_eq!(entry.dimension(), 4096);
    }

    // ================================================================
    // VectorMatch serialization
    // ================================================================

    #[test]
    fn test_vector_match_serialization_roundtrip() {
        let m = VectorMatch::new(
            "key".to_string(),
            0.875,
            Some(serde_json::json!({"source": "test"})),
        );
        let json = serde_json::to_string(&m).unwrap();
        let restored: VectorMatch = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.key, "key");
        assert!((restored.score - 0.875).abs() < f32::EPSILON);
        assert!(restored.metadata.is_some());
    }

    #[test]
    fn test_vector_match_negative_score() {
        // Cosine similarity can be negative
        let m = VectorMatch::new("neg".to_string(), -0.5, None);
        assert!(m.score < 0.0);
    }

    // ================================================================
    // VectorId edge cases
    // ================================================================

    #[test]
    fn test_vector_id_max() {
        let id = VectorId::new(u64::MAX);
        assert_eq!(id.as_u64(), u64::MAX);
        assert_eq!(format!("{}", id), format!("VectorId({})", u64::MAX));
    }

    #[test]
    fn test_vector_id_serialization_roundtrip() {
        let id = VectorId::new(12345);
        let json = serde_json::to_string(&id).unwrap();
        let restored: VectorId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, restored);
    }

    // ================================================================
    // DistanceMetric serde
    // ================================================================

    #[test]
    fn test_distance_metric_serde_roundtrip() {
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            let json = serde_json::to_string(&metric).unwrap();
            let restored: DistanceMetric = serde_json::from_str(&json).unwrap();
            assert_eq!(metric, restored);
        }
    }

    #[test]
    fn test_distance_metric_parse_empty_string() {
        assert_eq!(DistanceMetric::parse(""), None);
    }

    #[test]
    fn test_distance_metric_from_byte_reserved_values() {
        // Bytes 3+ are not assigned - should return None
        for b in 3..=255u8 {
            assert!(
                DistanceMetric::from_byte(b).is_none(),
                "Byte {} should not map to a metric",
                b
            );
        }
    }

    // ================================================================
    // StorageDtype serde
    // ================================================================

    #[test]
    fn test_storage_dtype_serde_roundtrip() {
        let dtype = StorageDtype::F32;
        let json = serde_json::to_string(&dtype).unwrap();
        let restored: StorageDtype = serde_json::from_str(&json).unwrap();
        assert_eq!(dtype, restored);
    }

    #[test]
    fn test_storage_dtype_from_byte_reserved_values() {
        // Byte 1 is reserved for future F16
        assert!(StorageDtype::from_byte(1).is_none());
        // Byte 2 is Int8
        assert_eq!(StorageDtype::from_byte(2), Some(StorageDtype::Int8));
        // Byte 3 is Binary
        assert_eq!(StorageDtype::from_byte(3), Some(StorageDtype::Binary));
        // Bytes 4+ are reserved
        for b in 4..=255u8 {
            assert!(
                StorageDtype::from_byte(b).is_none(),
                "Byte {} should not map to a dtype",
                b
            );
        }
    }

    // ================================================================
    // JsonScalar edge cases
    // ================================================================

    #[test]
    fn test_json_scalar_number_nan_does_not_match() {
        // NaN != NaN, so a NaN JsonScalar should not match a NaN JSON value
        let scalar = JsonScalar::Number(f64::NAN);
        // serde_json doesn't support NaN, so test with a normal number
        assert!(!scalar.matches_json(&serde_json::json!(42)));
        assert!(!scalar.matches_json(&serde_json::json!(f64::NAN))); // json!(NaN) → null
    }

    #[test]
    fn test_json_scalar_from_i64_large() {
        let s: JsonScalar = i64::MAX.into();
        if let JsonScalar::Number(n) = s {
            // i64::MAX loses precision when cast to f64
            assert!(n > 0.0);
        } else {
            panic!("Expected Number variant");
        }
    }

    #[test]
    fn test_json_scalar_from_f32() {
        let s: JsonScalar = 1.5f32.into();
        if let JsonScalar::Number(n) = s {
            assert!((n - 1.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected Number variant");
        }
    }

    #[test]
    fn test_json_scalar_string_empty() {
        let s: JsonScalar = "".into();
        assert!(s.matches_json(&serde_json::json!("")));
        assert!(!s.matches_json(&serde_json::json!(" ")));
    }

    // ================================================================
    // CollectionId edge cases
    // ================================================================

    #[test]
    fn test_collection_id_same_branch_different_name() {
        let branch_id = BranchId::new();
        let c1 = CollectionId::new(branch_id, "default", "alpha");
        let c2 = CollectionId::new(branch_id, "default", "beta");
        assert_ne!(c1, c2);
        assert!(c1 < c2, "alpha should sort before beta");
    }

    #[test]
    fn test_collection_id_hash() {
        use std::collections::HashSet;
        let branch_id = BranchId::new();
        let mut set = HashSet::new();
        set.insert(CollectionId::new(branch_id, "default", "a"));
        set.insert(CollectionId::new(branch_id, "default", "a")); // duplicate
        set.insert(CollectionId::new(branch_id, "default", "b"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_collection_id_serialization_roundtrip() {
        let branch_id = BranchId::new();
        let cid = CollectionId::new(branch_id, "default", "my_collection");
        let json = serde_json::to_string(&cid).unwrap();
        let restored: CollectionId = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, restored);
    }

    // ================================================================
    // VectorConfig edge cases
    // ================================================================

    #[test]
    fn test_vector_config_dimension_one() {
        let config = VectorConfig::new(1, DistanceMetric::DotProduct).unwrap();
        assert_eq!(config.dimension, 1);
    }

    #[test]
    fn test_vector_config_different_metrics_not_equal() {
        let c1 = VectorConfig::new(128, DistanceMetric::Cosine).unwrap();
        let c2 = VectorConfig::new(128, DistanceMetric::Euclidean).unwrap();
        assert_ne!(c1, c2);
    }
}
