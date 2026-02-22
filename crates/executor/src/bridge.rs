//! Bridge module: direct access to engine primitives.
//!
//! This module replaces the `strata-api` SubstrateImpl dependency with direct
//! engine primitive access. It provides:
//!
//! - [`Primitives`]: Holds all 6 engine primitives + database reference
//! - [`to_core_branch_id`]: Converts executor's string-based BranchId to core BranchId
//! - Validation helpers: Key, stream, event payload, collection name validation
//! - Type conversion helpers: Value ↔ JsonValue, DistanceMetric, etc.

use std::sync::Arc;

use strata_core::limits::Limits;
use strata_core::primitives::json::{JsonPath, JsonValue};
use strata_core::{StrataError, StrataResult, Value};
use strata_engine::{
    BranchIndex as PrimitiveBranchIndex, Database, EventLog as PrimitiveEventLog, GraphStore,
    JsonStore as PrimitiveJsonStore, KVStore as PrimitiveKVStore,
    SpaceIndex as PrimitiveSpaceIndex, StateCell as PrimitiveStateCell,
    VectorStore as PrimitiveVectorStore,
};

use crate::types::BranchId;

// =============================================================================
// Primitives
// =============================================================================

/// Direct access to all engine primitives.
///
/// Replaces `SubstrateImpl` from `strata-api`. Holds references to the database
/// and all 6 primitive stores, enabling direct engine calls without the API layer.
#[derive(Clone)]
pub struct Primitives {
    /// The underlying database
    pub db: Arc<Database>,
    /// KV primitive
    pub kv: PrimitiveKVStore,
    /// JSON primitive
    pub json: PrimitiveJsonStore,
    /// Event primitive
    pub event: PrimitiveEventLog,
    /// State primitive
    pub state: PrimitiveStateCell,
    /// Branch primitive
    pub branch: PrimitiveBranchIndex,
    /// Vector primitive
    pub vector: PrimitiveVectorStore,
    /// Space primitive
    pub space: PrimitiveSpaceIndex,
    /// Graph primitive
    pub graph: GraphStore,
    /// Size limits for keys, values, and vectors
    pub limits: Limits,
}

impl Primitives {
    /// Create primitives from a database instance.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            kv: PrimitiveKVStore::new(db.clone()),
            json: PrimitiveJsonStore::new(db.clone()),
            event: PrimitiveEventLog::new(db.clone()),
            state: PrimitiveStateCell::new(db.clone()),
            branch: PrimitiveBranchIndex::new(db.clone()),
            vector: PrimitiveVectorStore::new(db.clone()),
            space: PrimitiveSpaceIndex::new(db.clone()),
            graph: GraphStore::new(db.clone()),
            db,
            limits: Limits::default(),
        }
    }
}

// =============================================================================
// BranchId Conversion
// =============================================================================

/// Namespace UUID for generating deterministic branch IDs.
/// This is a fixed UUID used as the namespace for UUID v5 generation.
const BRANCH_NAMESPACE: uuid::Uuid = uuid::Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Convert executor's string-based BranchId to core BranchId.
///
/// - "default" → `BranchId` with UUID::nil (all zeros)
/// - Valid UUID string → `BranchId` with parsed UUID bytes
/// - Any other string → `BranchId` with deterministic UUID v5 generated from name
///
/// This allows users to use human-readable branch names like "main", "experiment-1",
/// etc. while still providing a unique UUID for internal namespacing.
pub fn to_core_branch_id(branch: &BranchId) -> crate::Result<strata_core::types::BranchId> {
    let s = branch.as_str();
    if s == "default" {
        Ok(strata_core::types::BranchId::from_bytes([0u8; 16]))
    } else if let Ok(u) = uuid::Uuid::parse_str(s) {
        // If it's already a valid UUID, use it directly
        Ok(strata_core::types::BranchId::from_bytes(*u.as_bytes()))
    } else {
        // Generate a deterministic UUID v5 from the branch name
        let uuid = uuid::Uuid::new_v5(&BRANCH_NAMESPACE, s.as_bytes());
        Ok(strata_core::types::BranchId::from_bytes(*uuid.as_bytes()))
    }
}

// =============================================================================
// Validation Helpers
// =============================================================================

/// Reserved key prefix that users cannot use.
const RESERVED_KEY_PREFIX: &str = "_strata/";

/// Validate a KV/JSON key.
///
/// Keys must be non-empty, contain no NUL bytes, not start with `_strata/`,
/// and not exceed the configured maximum key length.
pub fn validate_key(key: &str) -> StrataResult<()> {
    validate_key_with_limits(key, &Limits::default())
}

/// Validate a KV/JSON key against specific limits.
pub fn validate_key_with_limits(key: &str, limits: &Limits) -> StrataResult<()> {
    if key.is_empty() {
        return Err(StrataError::invalid_input("Key must not be empty"));
    }
    if let Err(e) = limits.validate_key_length(key) {
        return Err(StrataError::capacity_exceeded("key", e.max(), e.actual()));
    }
    if key.contains('\0') {
        return Err(StrataError::invalid_input("Key must not contain NUL bytes"));
    }
    if key.starts_with(RESERVED_KEY_PREFIX) {
        return Err(StrataError::invalid_input(format!(
            "Key must not start with reserved prefix '{}'",
            RESERVED_KEY_PREFIX
        )));
    }
    Ok(())
}

/// Validate a value against size limits.
pub fn validate_value(value: &Value, limits: &Limits) -> StrataResult<()> {
    limits.validate_value(value).map_err(limit_error_to_strata)
}

/// Validate a vector against dimension limits.
pub fn validate_vector(vec: &[f32], limits: &Limits) -> StrataResult<()> {
    limits.validate_vector(vec).map_err(limit_error_to_strata)
}

/// Convert a `LimitError` to a `StrataError`.
fn limit_error_to_strata(e: strata_core::limits::LimitError) -> StrataError {
    StrataError::capacity_exceeded(e.reason_code(), e.max(), e.actual())
}
/// Check if a collection name is internal (starts with `_`).
pub(crate) fn is_internal_collection(name: &str) -> bool {
    name.starts_with('_')
}

/// Validate that a collection name is not internal.
pub fn validate_not_internal_collection(name: &str) -> StrataResult<()> {
    if is_internal_collection(name) {
        return Err(StrataError::invalid_input(format!(
            "Collection '{}' is internal and cannot be accessed directly",
            name
        )));
    }
    Ok(())
}

// =============================================================================
// Type Conversion: Value ↔ JsonValue
// =============================================================================

/// Convert `strata_core::Value` to `JsonValue` for the JSON primitive.
pub fn value_to_json(value: Value) -> StrataResult<JsonValue> {
    let json_val = value_to_serde_json(value)?;
    Ok(JsonValue::from(json_val))
}

/// Convert `JsonValue` back to `strata_core::Value`.
pub fn json_to_value(json: JsonValue) -> StrataResult<Value> {
    let serde_val: serde_json::Value = json.into();
    serde_json_to_value(serde_val)
}

/// Convert a Value to serde_json::Value without serde's tagged enum format.
fn value_to_serde_json(value: Value) -> StrataResult<serde_json::Value> {
    use serde_json::Map;
    use serde_json::Value as JV;

    match value {
        Value::Null => Ok(JV::Null),
        Value::Bool(b) => Ok(JV::Bool(b)),
        Value::Int(i) => Ok(JV::Number(i.into())),
        Value::Float(f) => {
            if f.is_infinite() || f.is_nan() {
                return Err(StrataError::serialization(format!(
                    "Cannot convert {} to JSON: not a valid JSON number",
                    f
                )));
            }
            serde_json::Number::from_f64(f)
                .map(JV::Number)
                .ok_or_else(|| {
                    StrataError::serialization(format!("Cannot convert {} to JSON number", f))
                })
        }
        Value::String(s) => Ok(JV::String(s)),
        Value::Bytes(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&b);
            Ok(JV::String(format!("__bytes__:{}", encoded)))
        }
        Value::Array(arr) => {
            let converted: Result<Vec<_>, _> = arr.into_iter().map(value_to_serde_json).collect();
            Ok(JV::Array(converted?))
        }
        Value::Object(obj) => {
            let mut map = Map::new();
            for (k, v) in obj {
                map.insert(k, value_to_serde_json(v)?);
            }
            Ok(JV::Object(map))
        }
    }
}

/// Convert serde_json::Value to Value without serde deserialization.
fn serde_json_to_value(json: serde_json::Value) -> StrataResult<Value> {
    use serde_json::Value as JV;

    match json {
        JV::Null => Ok(Value::Null),
        JV::Bool(b) => Ok(Value::Bool(b)),
        JV::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(StrataError::serialization(format!(
                    "Cannot convert JSON number {} to Value",
                    n
                )))
            }
        }
        JV::String(s) => {
            if let Some(encoded) = s.strip_prefix("__bytes__:") {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| {
                        StrataError::serialization(format!("Invalid base64 in bytes value: {}", e))
                    })?;
                Ok(Value::Bytes(bytes))
            } else {
                Ok(Value::String(s))
            }
        }
        JV::Array(arr) => {
            let converted: Result<Vec<_>, _> = arr.into_iter().map(serde_json_to_value).collect();
            Ok(Value::Array(converted?))
        }
        JV::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(k, serde_json_to_value(v)?);
            }
            Ok(Value::Object(map))
        }
    }
}

/// Parse a string path to JsonPath.
///
/// Supports standard JSONPath `$` prefix:
/// - `"$"` or `""` → root
/// - `"$.name"` → field access (strips `$` prefix)
/// - `"$[0]"` → array index (strips `$` prefix)
/// - `"name"` or `".name"` → field access (no prefix)
pub fn parse_path(path: &str) -> StrataResult<JsonPath> {
    if path.is_empty() || path == "$" {
        return Ok(JsonPath::root());
    }
    // Strip leading "$" so "$.name" → ".name", "$[0]" → "[0]"
    let normalized = path.strip_prefix('$').unwrap_or(path);
    normalized
        .parse()
        .map_err(|e| StrataError::invalid_input(format!("Invalid JSON path '{}': {:?}", path, e)))
}

// =============================================================================
// Version Helpers
// =============================================================================

/// Extract u64 from a Version enum.
pub fn extract_version(v: &strata_core::Version) -> u64 {
    match v {
        strata_core::Version::Txn(n) => *n,
        strata_core::Version::Sequence(n) => *n,
        strata_core::Version::Counter(n) => *n,
    }
}

/// Convert a `Versioned<Value>` to executor's `VersionedValue`.
pub fn to_versioned_value(v: strata_core::Versioned<Value>) -> crate::types::VersionedValue {
    crate::types::VersionedValue {
        value: v.value,
        version: extract_version(&v.version),
        timestamp: v.timestamp.into(),
    }
}

// =============================================================================
// DistanceMetric Conversion
// =============================================================================

/// Convert executor DistanceMetric to engine DistanceMetric.
pub fn to_engine_metric(metric: crate::types::DistanceMetric) -> strata_engine::DistanceMetric {
    match metric {
        crate::types::DistanceMetric::Cosine => strata_engine::DistanceMetric::Cosine,
        crate::types::DistanceMetric::Euclidean => strata_engine::DistanceMetric::Euclidean,
        crate::types::DistanceMetric::DotProduct => strata_engine::DistanceMetric::DotProduct,
    }
}

/// Convert engine DistanceMetric to executor DistanceMetric.
pub fn from_engine_metric(metric: strata_engine::DistanceMetric) -> crate::types::DistanceMetric {
    match metric {
        strata_engine::DistanceMetric::Cosine => crate::types::DistanceMetric::Cosine,
        strata_engine::DistanceMetric::Euclidean => crate::types::DistanceMetric::Euclidean,
        strata_engine::DistanceMetric::DotProduct => crate::types::DistanceMetric::DotProduct,
    }
}

// =============================================================================
// SearchFilter Conversion
// =============================================================================

/// Convert executor MetadataFilter list to engine MetadataFilter.
pub fn to_engine_filter(
    filters: &[crate::types::MetadataFilter],
) -> Option<strata_engine::MetadataFilter> {
    if filters.is_empty() {
        return None;
    }

    let mut engine_filter = strata_engine::MetadataFilter::new();

    for f in filters {
        let scalar = value_to_json_scalar(&f.value);
        match f.op {
            crate::types::FilterOp::Eq => {
                engine_filter.equals.insert(f.field.clone(), scalar);
            }
            _ => {
                let engine_op = match f.op {
                    crate::types::FilterOp::Eq => strata_engine::FilterOp::Eq,
                    crate::types::FilterOp::Ne => strata_engine::FilterOp::Ne,
                    crate::types::FilterOp::Gt => strata_engine::FilterOp::Gt,
                    crate::types::FilterOp::Gte => strata_engine::FilterOp::Gte,
                    crate::types::FilterOp::Lt => strata_engine::FilterOp::Lt,
                    crate::types::FilterOp::Lte => strata_engine::FilterOp::Lte,
                    crate::types::FilterOp::In => strata_engine::FilterOp::In,
                    crate::types::FilterOp::Contains => strata_engine::FilterOp::Contains,
                };
                engine_filter
                    .conditions
                    .push(strata_engine::FilterCondition {
                        field: f.field.clone(),
                        op: engine_op,
                        value: scalar,
                    });
            }
        }
    }

    if engine_filter.is_empty() {
        None
    } else {
        Some(engine_filter)
    }
}

/// Convert a Value to a JsonScalar for vector metadata filtering.
fn value_to_json_scalar(value: &Value) -> strata_engine::JsonScalar {
    match value {
        Value::Null => strata_engine::JsonScalar::Null,
        Value::Bool(b) => strata_engine::JsonScalar::Bool(*b),
        Value::Int(i) => strata_engine::JsonScalar::Number(*i as f64),
        Value::Float(f) => strata_engine::JsonScalar::Number(*f),
        Value::String(s) => strata_engine::JsonScalar::String(s.clone()),
        _ => strata_engine::JsonScalar::Null,
    }
}

// =============================================================================
// BranchStatus Conversion
// =============================================================================

/// Convert engine BranchStatus to executor BranchStatus.
pub fn from_engine_branch_status(
    status: strata_engine::BranchStatus,
) -> crate::types::BranchStatus {
    match status {
        strata_engine::BranchStatus::Active => crate::types::BranchStatus::Active,
    }
}

// =============================================================================
// Value ↔ serde_json::Value for vector metadata
// =============================================================================

/// Convert `Value` to `serde_json::Value` for vector metadata storage.
pub fn value_to_serde_json_public(value: Value) -> StrataResult<serde_json::Value> {
    value_to_serde_json(value)
}

/// Convert `serde_json::Value` to `Value` for vector metadata retrieval.
pub fn serde_json_to_value_public(json: serde_json::Value) -> StrataResult<Value> {
    serde_json_to_value(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_core_branch_id_default() {
        let branch = BranchId::from("default");
        let core_id = to_core_branch_id(&branch).unwrap();
        assert_eq!(core_id.as_bytes(), &[0u8; 16]);
    }

    #[test]
    fn test_to_core_branch_id_uuid() {
        let branch = BranchId::from("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        let core_id = to_core_branch_id(&branch).unwrap();
        let expected = uuid::Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
        assert_eq!(core_id.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn test_to_core_branch_id_name_generates_v5_uuid() {
        // Non-UUID names generate a deterministic UUID v5
        let branch = BranchId::from("not-a-valid-id");
        let result = to_core_branch_id(&branch);
        assert!(
            result.is_ok(),
            "Arbitrary names should generate valid v5 UUIDs"
        );

        // Same name should produce same UUID (deterministic)
        let branch2 = BranchId::from("not-a-valid-id");
        let result2 = to_core_branch_id(&branch2).unwrap();
        assert_eq!(result.unwrap().as_bytes(), result2.as_bytes());
    }

    #[test]
    fn test_validate_key_valid() {
        assert!(validate_key("hello").is_ok());
        assert!(validate_key("a/b/c").is_ok());
    }

    #[test]
    fn test_validate_key_empty() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn test_validate_key_reserved() {
        assert!(validate_key("_strata/internal").is_err());
    }

    #[test]
    fn test_validate_key_nul() {
        assert!(validate_key("hello\0world").is_err());
    }

    #[test]
    fn test_validate_key_too_long() {
        let long_key = "a".repeat(1025);
        assert!(validate_key(&long_key).is_err());
    }

    #[test]
    fn test_value_json_roundtrip() {
        let value = Value::Int(42);
        let json = value_to_json(value).unwrap();
        let restored = json_to_value(json).unwrap();
        assert_eq!(restored, Value::Int(42));
    }

    #[test]
    fn test_extract_version_variants() {
        use strata_core::Version;
        assert_eq!(extract_version(&Version::Txn(42)), 42);
        assert_eq!(extract_version(&Version::Sequence(100)), 100);
        assert_eq!(extract_version(&Version::Counter(7)), 7);
    }
}
