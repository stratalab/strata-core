//! Vector input types and metadata filter helpers.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::diagnostics::{EngineError, EngineResult};

const MAX_COLLECTION_NAME_BYTES: usize = 256;
const MAX_VECTOR_KEY_BYTES: usize = 1024;
pub(crate) const MAX_VECTOR_DIMENSION: usize = 32_768;
const MAX_EMBEDDING_MODEL_BYTES: usize = 256;
const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;

/// Vector collection name.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct VectorCollectionName(String);

impl VectorCollectionName {
    /// Creates a validated vector collection name.
    pub fn new(name: impl Into<String>) -> Result<Self, EngineError> {
        let name = name.into();
        validate_text_component(
            &name,
            MAX_COLLECTION_NAME_BYTES,
            false,
            "invalid_argument.engine.vector_collection",
            "vector collection name",
        )?;
        if name.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_collection_reserved",
                "vector collection name is reserved for engine internals",
            ));
        }
        if name.contains('/') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_collection",
                "vector collection name must not contain `/`",
            ));
        }
        Ok(Self(name))
    }

    #[must_use]
    /// Returns the collection name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for VectorCollectionName {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for VectorCollectionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VectorCollectionName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Vector key within one collection.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct VectorKey(String);

impl VectorKey {
    /// Creates a validated vector key.
    pub fn new(key: impl Into<String>) -> Result<Self, EngineError> {
        let key = key.into();
        validate_text_component(
            &key,
            MAX_VECTOR_KEY_BYTES,
            true,
            "invalid_argument.engine.vector_key",
            "vector key",
        )?;
        Ok(Self(key))
    }

    #[must_use]
    /// Returns the key as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for VectorKey {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for VectorKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VectorKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Dense vector embedding.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct VectorEmbedding(Vec<f32>);

impl VectorEmbedding {
    /// Creates a validated embedding.
    pub fn new(values: impl Into<Vec<f32>>) -> Result<Self, EngineError> {
        let values = values.into();
        validate_embedding_values(&values)?;
        Ok(Self(values))
    }

    #[must_use]
    /// Returns embedding values.
    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }

    #[must_use]
    /// Returns the embedding dimension.
    pub fn dimension(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    /// Consumes the embedding and returns raw values.
    pub fn into_values(self) -> Vec<f32> {
        self.0
    }

    pub(crate) fn validate_dimension(&self, expected: usize) -> Result<(), EngineError> {
        if self.dimension() != expected {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_dimension",
                format!(
                    "vector dimension mismatch: expected {expected}, got {}",
                    self.dimension()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn from_stored(values: Vec<f32>) -> Result<Self, EngineError> {
        Self::new(values)
    }

    /// Creates a validated embedding from wire-precision f64 values, narrowing to
    /// the stored f32 representation. A value that cannot be represented in f32
    /// without becoming non-finite or losing a non-zero magnitude to underflow is
    /// rejected with `invalid_argument.engine.vector_embedding`, so the caller is
    /// told rather than a corrupted (silently zeroed) vector being stored.
    pub fn from_wire(values: Vec<f64>) -> Result<Self, EngineError> {
        let narrowed = values
            .into_iter()
            .map(narrow_embedding_value)
            .collect::<EngineResult<Vec<f32>>>()?;
        Self::new(narrowed)
    }
}

impl<'de> Deserialize<'de> for VectorEmbedding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::<f32>::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// JSON metadata stored alongside a vector.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct VectorMetadata(Value);

impl VectorMetadata {
    /// Creates validated vector metadata.
    pub fn new(value: Value) -> Result<Self, EngineError> {
        if !value.is_object() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_metadata",
                "vector metadata must be a JSON object",
            ));
        }
        let size = serde_json::to_vec(&value)
            .map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.vector_metadata",
                    format!("vector metadata cannot be serialized: {error}"),
                )
            })?
            .len();
        if size > MAX_METADATA_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_metadata_too_large",
                "vector metadata exceeds the maximum encoded size",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    /// Returns the wrapped metadata value.
    pub fn as_inner(&self) -> &Value {
        &self.0
    }

    #[must_use]
    /// Consumes the wrapper and returns the JSON value.
    pub fn into_inner(self) -> Value {
        self.0
    }

    pub(crate) fn clone_inner(&self) -> Value {
        self.0.clone()
    }
}

impl TryFrom<Value> for VectorMetadata {
    type Error = EngineError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for VectorMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Vector distance metric.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorDistanceMetric {
    /// Cosine similarity.
    #[default]
    Cosine,
    /// Euclidean similarity, expressed as `1 / (1 + l2_distance)`.
    Euclidean,
    /// Raw dot product.
    DotProduct,
}

/// Which embedding model produced a collection's vectors.
///
/// Provenance, not a capability claim: engine cannot and must not check that
/// the model exists — it never speaks to a provider (hard rules 2-3). What it
/// can do is refuse to mix two models' vectors in one collection, which is the
/// failure dimension checks cannot catch. `miniLM` and `nomic-embed` at the
/// same width produce neighbours that rank confidently and mean nothing.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct EmbeddingModelId(String);

impl EmbeddingModelId {
    /// Creates a validated embedding model identifier.
    ///
    /// Accepts any model spec the caller uses — `miniLM`,
    /// `openai:text-embedding-3-small` — because the shape belongs to the
    /// inference layer, which engine cannot see.
    pub fn new(id: impl Into<String>) -> Result<Self, EngineError> {
        let id = id.into();
        if id.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.embedding_model",
                "embedding model must not be empty",
            ));
        }
        if id.len() > MAX_EMBEDDING_MODEL_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.embedding_model",
                "embedding model is too long",
            ));
        }
        if id.bytes().any(|byte| byte == 0 || byte == b'\n') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.embedding_model",
                "embedding model contains an unsupported control byte",
            ));
        }
        Ok(Self(id))
    }

    #[must_use]
    /// Returns the model identifier as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for EmbeddingModelId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Immutable vector collection config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VectorConfig {
    dimension: usize,
    metric: VectorDistanceMetric,
    // Not `Copy` since this landed: an owned model id is what lets a
    // collection say what produced it (#3124 D9, CLAUDE.md rule 24).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    embedding_model: Option<EmbeddingModelId>,
}

impl VectorConfig {
    /// Creates a validated vector collection config.
    pub fn new(dimension: usize, metric: VectorDistanceMetric) -> Result<Self, EngineError> {
        if dimension == 0 || dimension > MAX_VECTOR_DIMENSION {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_dimension",
                "vector collection dimension is outside the supported range",
            ));
        }
        Ok(Self {
            dimension,
            metric,
            embedding_model: None,
        })
    }

    #[must_use]
    /// Records which embedding model produces this collection's vectors.
    ///
    /// A collection without one accepts any vector of the right width, which
    /// is what every collection created before this did — so it stays legal,
    /// and only text-embedding paths that need to know the model refuse.
    pub fn with_embedding_model(mut self, model: EmbeddingModelId) -> Self {
        self.embedding_model = Some(model);
        self
    }

    #[must_use]
    /// Returns the collection embedding dimension.
    pub const fn dimension(&self) -> usize {
        self.dimension
    }

    #[must_use]
    /// Returns the collection distance metric.
    pub const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    #[must_use]
    /// Returns the model that produces this collection's vectors, if recorded.
    pub fn embedding_model(&self) -> Option<&EmbeddingModelId> {
        self.embedding_model.as_ref()
    }
}

impl<'de> Deserialize<'de> for VectorConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoredConfig {
            dimension: usize,
            metric: VectorDistanceMetric,
            #[serde(default)]
            embedding_model: Option<EmbeddingModelId>,
        }

        let stored = StoredConfig::deserialize(deserializer)?;
        let config =
            Self::new(stored.dimension, stored.metric).map_err(serde::de::Error::custom)?;
        // `default` on the field is what makes every collection written before
        // D9 decode: no model recorded, which is a legal state.
        Ok(match stored.embedding_model {
            Some(model) => config.with_embedding_model(model),
            None => config,
        })
    }
}

/// Scalar value used by vector metadata filters.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum VectorScalar {
    /// JSON null.
    Null,
    /// Boolean scalar.
    Bool(bool),
    /// Numeric scalar compared as an f64.
    Number(f64),
    /// String scalar.
    String(String),
}

impl<'de> Deserialize<'de> for VectorScalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case", tag = "type", content = "value")]
        enum StoredScalar {
            Null,
            Bool(bool),
            Number(f64),
            String(String),
        }

        match StoredScalar::deserialize(deserializer)? {
            StoredScalar::Null => Ok(Self::Null),
            StoredScalar::Bool(value) => Ok(Self::Bool(value)),
            StoredScalar::Number(value) if value.is_finite() => Ok(Self::Number(value)),
            StoredScalar::Number(_) => Err(serde::de::Error::custom(
                "vector metadata filter number must be finite",
            )),
            StoredScalar::String(value) => Ok(Self::String(value)),
        }
    }
}

impl VectorScalar {
    #[must_use]
    /// Returns true when this scalar equals the JSON value.
    pub fn matches_json(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Null, Value::Null) => true,
            (Self::Bool(expected), Value::Bool(actual)) => expected == actual,
            (Self::Number(expected), Value::Number(actual)) => actual
                .as_f64()
                .is_some_and(|actual| actual.to_bits() == expected.to_bits()),
            (Self::String(expected), Value::String(actual)) => expected == actual,
            _ => false,
        }
    }
}

impl From<bool> for VectorScalar {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for VectorScalar {
    fn from(value: i32) -> Self {
        Self::Number(f64::from(value))
    }
}

impl From<f32> for VectorScalar {
    fn from(value: f32) -> Self {
        Self::Number(f64::from(value))
    }
}

impl From<f64> for VectorScalar {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<&str> for VectorScalar {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for VectorScalar {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Metadata filter operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorFilterOp {
    /// Top-level equality.
    Eq,
}

/// One metadata filter condition.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct VectorFilterCondition {
    field: String,
    op: VectorFilterOp,
    value: VectorScalar,
}

impl<'de> Deserialize<'de> for VectorFilterCondition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoredCondition {
            field: String,
            op: VectorFilterOp,
            value: VectorScalar,
        }

        let stored = StoredCondition::deserialize(deserializer)?;
        Self::new(stored.field, stored.op, stored.value).map_err(serde::de::Error::custom)
    }
}

impl VectorFilterCondition {
    /// Creates a top-level metadata filter condition.
    pub fn new(
        field: impl Into<String>,
        op: VectorFilterOp,
        value: impl Into<VectorScalar>,
    ) -> Result<Self, EngineError> {
        let field = field.into();
        validate_metadata_field(&field)?;
        let value = value.into();
        if let VectorScalar::Number(number) = &value {
            if !number.is_finite() {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.vector_filter",
                    "vector metadata filter number must be finite",
                ));
            }
        }
        Ok(Self { field, op, value })
    }

    #[must_use]
    /// Returns the metadata field name.
    pub fn field(&self) -> &str {
        &self.field
    }

    #[must_use]
    /// Returns the filter operation.
    pub const fn op(&self) -> VectorFilterOp {
        self.op
    }

    #[must_use]
    /// Returns the scalar comparison value.
    pub const fn value(&self) -> &VectorScalar {
        &self.value
    }

    fn matches(&self, object: &Map<String, Value>) -> bool {
        match self.op {
            VectorFilterOp::Eq => object
                .get(&self.field)
                .is_some_and(|actual| self.value.matches_json(actual)),
        }
    }
}

/// AND-composed vector metadata filter.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct VectorFilter(Vec<VectorFilterCondition>);

impl VectorFilter {
    /// Creates an empty filter.
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Adds an equality condition.
    pub fn eq(
        mut self,
        field: impl Into<String>,
        value: impl Into<VectorScalar>,
    ) -> Result<Self, EngineError> {
        let condition = VectorFilterCondition::new(field, VectorFilterOp::Eq, value)?;
        self.0.push(condition);
        Ok(self)
    }

    /// Creates a filter from validated conditions.
    pub fn from_conditions(conditions: Vec<VectorFilterCondition>) -> Self {
        Self(conditions)
    }

    #[must_use]
    /// Returns filter conditions.
    pub fn conditions(&self) -> &[VectorFilterCondition] {
        &self.0
    }

    #[must_use]
    /// Returns true when the filter has no conditions.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    /// Returns the number of conditions.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    /// Returns true when this filter matches the optional metadata value.
    pub fn matches(&self, metadata: Option<&VectorMetadata>) -> bool {
        if self.is_empty() {
            return true;
        }
        let Some(Value::Object(object)) = metadata.map(VectorMetadata::as_inner) else {
            return false;
        };
        self.0.iter().all(|condition| condition.matches(object))
    }
}

impl<'de> Deserialize<'de> for VectorFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(Vec::<VectorFilterCondition>::deserialize(
            deserializer,
        )?))
    }
}

/// Top-level metadata patch.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct VectorMetadataPatch(Map<String, Value>);

impl VectorMetadataPatch {
    /// Creates a validated metadata patch from a JSON object.
    pub fn new(value: Value) -> Result<Self, EngineError> {
        let Value::Object(map) = value else {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_metadata_patch",
                "vector metadata patch must be a JSON object",
            ));
        };
        Self::from_map(map)
    }

    /// Creates a validated metadata patch from an object map.
    pub fn from_map(map: Map<String, Value>) -> Result<Self, EngineError> {
        if map.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.vector_metadata_patch",
                "vector metadata patch must contain at least one field",
            ));
        }
        for key in map.keys() {
            validate_metadata_field(key)?;
        }
        let value = Value::Object(map.clone());
        let _ = VectorMetadata::new(value)?;
        Ok(Self(map))
    }

    #[must_use]
    /// Returns the patch fields.
    pub const fn fields(&self) -> &Map<String, Value> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for VectorMetadataPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// One vector batch upsert entry.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct VectorUpsertEntry {
    key: VectorKey,
    embedding: VectorEmbedding,
    metadata: Option<VectorMetadata>,
}

impl<'de> Deserialize<'de> for VectorUpsertEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoredEntry {
            key: VectorKey,
            embedding: VectorEmbedding,
            metadata: Option<VectorMetadata>,
        }

        let stored = StoredEntry::deserialize(deserializer)?;
        Ok(Self::new(stored.key, stored.embedding, stored.metadata))
    }
}

impl VectorUpsertEntry {
    /// Creates a vector batch upsert entry.
    pub const fn new(
        key: VectorKey,
        embedding: VectorEmbedding,
        metadata: Option<VectorMetadata>,
    ) -> Self {
        Self {
            key,
            embedding,
            metadata,
        }
    }

    #[must_use]
    /// Returns the entry key.
    pub const fn key(&self) -> &VectorKey {
        &self.key
    }

    #[must_use]
    /// Returns the embedding.
    pub const fn embedding(&self) -> &VectorEmbedding {
        &self.embedding
    }

    #[must_use]
    /// Returns the optional metadata.
    pub const fn metadata(&self) -> Option<&VectorMetadata> {
        self.metadata.as_ref()
    }
}

fn validate_text_component(
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
    code: &'static str,
    label: &'static str,
) -> Result<(), EngineError> {
    if !allow_empty && value.is_empty() {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} must not be empty"),
        ));
    }
    if value.len() > max_bytes {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} is too long"),
        ));
    }
    if value.bytes().any(|byte| byte == 0) {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} contains an unsupported control byte"),
        ));
    }
    Ok(())
}

fn validate_metadata_field(field: &str) -> Result<(), EngineError> {
    validate_text_component(
        field,
        MAX_VECTOR_KEY_BYTES,
        false,
        "invalid_argument.engine.vector_metadata_field",
        "vector metadata field",
    )?;
    if field.contains('.') || field.contains('/') {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_metadata_field",
            "vector metadata field must be a top-level field name",
        ));
    }
    Ok(())
}

fn validate_embedding_values(values: &[f32]) -> Result<(), EngineError> {
    if values.is_empty() || values.len() > MAX_VECTOR_DIMENSION {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_embedding",
            "vector embedding dimension is outside the supported range",
        ));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_embedding",
            "vector embedding contains a non-finite value",
        ));
    }
    Ok(())
}

/// Narrows one wire-precision f64 component to the stored f32. A non-zero
/// magnitude that underflows to zero is rejected here — the case that otherwise
/// silently corrupted a stored vector, and which `VectorEmbedding::new` cannot
/// see (`0.0` is finite). Non-finite input and overflow to infinity are left to
/// `new`'s finiteness check, which rejects them with the same code.
fn narrow_embedding_value(value: f64) -> Result<f32, EngineError> {
    #[allow(clippy::cast_possible_truncation)]
    let narrowed = value as f32;
    if narrowed == 0.0 && value != 0.0 {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_embedding",
            "vector embedding value underflows to zero in f32",
        ));
    }
    Ok(narrowed)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorFilter,
        VectorFilterCondition, VectorFilterOp, VectorKey, VectorMetadataPatch, VectorScalar,
    };
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn from_wire_rejects_non_representable_values_and_keeps_genuine_zero() {
        // Overflow to infinity (the extreme that was already rejected).
        let overflow = VectorEmbedding::from_wire(vec![1e308, 0.0]).expect_err("overflow rejected");
        assert_eq!(overflow.class(), EngineErrorClass::InvalidInput);
        assert_eq!(overflow.code(), "invalid_argument.engine.vector_embedding");

        // Non-finite input.
        assert_eq!(
            VectorEmbedding::from_wire(vec![f64::INFINITY, 0.0])
                .expect_err("non-finite rejected")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );

        // The bug: a non-zero magnitude that underflows to zero in f32 must be
        // rejected, not silently stored as the zero vector.
        assert_eq!(
            VectorEmbedding::from_wire(vec![1e-308, 1e-308])
                .expect_err("subnormal underflow rejected")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );

        // A representable value keeps its magnitude; a genuine zero component is
        // fine, and an all-zero (degenerate) vector is still a valid embedding.
        assert_eq!(
            VectorEmbedding::from_wire(vec![0.5, 0.0])
                .expect("valid embedding")
                .as_slice(),
            [0.5_f32, 0.0]
        );
        assert!(VectorEmbedding::from_wire(vec![0.0, 0.0]).is_ok());

        // Dimension bounds still apply.
        assert!(VectorEmbedding::from_wire(Vec::new()).is_err());
    }

    #[test]
    fn vector_type_validation_rejects_invalid_inputs() {
        assert_eq!(
            VectorCollectionName::new("")
                .expect_err("empty collection")
                .class(),
            EngineErrorClass::InvalidInput
        );
        assert_eq!(
            VectorCollectionName::new("_internal")
                .expect_err("reserved collection")
                .code(),
            "invalid_argument.engine.vector_collection_reserved"
        );
        assert!(VectorKey::new("").is_ok());
        assert_eq!(
            VectorEmbedding::new([1.0, f32::NAN])
                .expect_err("non-finite embedding")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );
        assert_eq!(
            VectorConfig::new(0, VectorDistanceMetric::Cosine)
                .expect_err("zero dimension")
                .code(),
            "invalid_argument.engine.vector_dimension"
        );
        assert_eq!(
            VectorMetadataPatch::new(json!([]))
                .expect_err("non-object patch")
                .code(),
            "invalid_argument.engine.vector_metadata_patch"
        );
    }

    #[test]
    fn vector_text_types_validate_boundaries() {
        assert!(VectorCollectionName::new("docs").is_ok());
        assert!(VectorCollectionName::new("café").is_ok());
        assert!(VectorCollectionName::new("a".repeat(256)).is_ok());
        assert_eq!(
            VectorCollectionName::new("a".repeat(257))
                .expect_err("overlong collection")
                .code(),
            "invalid_argument.engine.vector_collection"
        );
        assert_eq!(
            VectorCollectionName::new("bad/name")
                .expect_err("separator rejected")
                .code(),
            "invalid_argument.engine.vector_collection"
        );
        assert_eq!(
            VectorCollectionName::new("bad\0name")
                .expect_err("null rejected")
                .code(),
            "invalid_argument.engine.vector_collection"
        );

        assert!(VectorKey::new("doc/1").is_ok());
        assert!(VectorKey::new("a".repeat(1024)).is_ok());
        assert_eq!(
            VectorKey::new("a".repeat(1025))
                .expect_err("overlong key")
                .code(),
            "invalid_argument.engine.vector_key"
        );
        assert_eq!(
            VectorKey::new("bad\0key")
                .expect_err("null rejected")
                .code(),
            "invalid_argument.engine.vector_key"
        );
    }

    #[test]
    fn vector_embedding_and_config_validate_boundaries() {
        assert!(VectorEmbedding::new([0.0, -1.0, 2.5]).is_ok());
        assert_eq!(
            VectorEmbedding::new(Vec::<f32>::new())
                .expect_err("empty embedding")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );
        assert_eq!(
            VectorEmbedding::new([f32::NEG_INFINITY])
                .expect_err("infinite embedding")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );
        assert_eq!(
            VectorEmbedding::new(vec![0.0; 32_769])
                .expect_err("overlong embedding")
                .code(),
            "invalid_argument.engine.vector_embedding"
        );
        assert!(VectorConfig::new(32_768, VectorDistanceMetric::Cosine).is_ok());
        assert_eq!(
            VectorConfig::new(32_769, VectorDistanceMetric::Cosine)
                .expect_err("overlong dimension")
                .code(),
            "invalid_argument.engine.vector_dimension"
        );
    }

    #[test]
    fn vector_metadata_patch_validation_is_top_level_only() {
        let mut overlong = serde_json::Map::new();
        overlong.insert("a".repeat(1025), json!(true));

        assert_eq!(
            VectorMetadataPatch::new(json!({}))
                .expect_err("empty patch")
                .code(),
            "invalid_argument.engine.vector_metadata_patch"
        );
        assert_eq!(
            VectorMetadataPatch::new(json!({"nested.path": true}))
                .expect_err("nested field")
                .code(),
            "invalid_argument.engine.vector_metadata_field"
        );
        assert_eq!(
            VectorMetadataPatch::new(json!({"nested/path": true}))
                .expect_err("path field")
                .code(),
            "invalid_argument.engine.vector_metadata_field"
        );
        assert_eq!(
            VectorMetadataPatch::new(serde_json::Value::Object(overlong))
                .expect_err("overlong field")
                .code(),
            "invalid_argument.engine.vector_metadata_field"
        );
        assert!(VectorMetadataPatch::new(json!({"field": null})).is_ok());
    }

    #[test]
    fn vector_filter_validation_is_top_level_scalar_only() {
        assert!(VectorFilter::new().matches(None));
        assert_eq!(
            VectorFilter::new()
                .eq("nested.path", true)
                .expect_err("nested field")
                .code(),
            "invalid_argument.engine.vector_metadata_field"
        );
        assert_eq!(
            VectorFilterCondition::new("score", VectorFilterOp::Eq, f64::NAN)
                .expect_err("non-finite scalar")
                .code(),
            "invalid_argument.engine.vector_filter"
        );
        assert!(serde_json::from_value::<VectorScalar>(json!({
            "type": "object",
            "value": {}
        }))
        .is_err());
        assert!(serde_json::from_value::<VectorFilterCondition>(json!({
            "field": "kind",
            "op": "eq",
            "value": ["doc"]
        }))
        .is_err());
    }

    #[test]
    fn vector_filter_matches_top_level_scalars() {
        let filter = VectorFilter::new()
            .eq("kind", "doc")
            .expect("valid filter")
            .eq("active", true)
            .expect("valid filter");
        let metadata = crate::data::vector::VectorMetadata::new(json!({
            "kind": "doc",
            "active": true,
            "ignored": ["nested"]
        }))
        .expect("valid metadata");
        assert!(filter.matches(Some(&metadata)));
        assert!(!filter.matches(None));
    }
}
