use super::{Deserialize, Serialize, Value};

/// JSON secondary index kind exposed through the command boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum JsonIndexType {
    /// Numeric field index.
    Numeric,
    /// Exact tag/string field index.
    Tag,
    /// Lowercase text field index.
    Text,
}

/// Stored JSON value with commit metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonVersionedValue {
    value: Value,
    version: u64,
    /// Logical commit-timeline position of the commit that wrote this value: a
    /// monotonic per-commit counter (a fresh database starts small, near 1),
    /// never a calendar date. Pass it to `--as-of` and match it against
    /// `history`; do not format it as a Unix/epoch timestamp.
    timestamp: u64,
    document_version: u64,
}

impl JsonVersionedValue {
    /// Creates a JSON versioned value.
    pub fn new(value: Value, version: u64, timestamp: u64, document_version: u64) -> Self {
        Self {
            value,
            version,
            timestamp,
            document_version,
        }
    }

    /// Returns the selected JSON value.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the document version.
    pub const fn document_version(&self) -> u64 {
        self.document_version
    }
}

/// JSON point-read result that distinguishes absence from a stored JSON null.
///
/// Serializes the same `{found, value}` wire shape as the shared
/// [`Maybe`](super::Maybe) envelope, but carries a non-optional `value` so a
/// stored JSON `null` (`found: true, value: null`) stays distinct from an
/// absent document (`found: false, value: null`). `Maybe<serde_json::Value>`
/// cannot express that distinction because `Option<Value>` deserializes a JSON
/// `null` back to absence — hence this dedicated type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct MaybeJsonValue {
    found: bool,
    value: Value,
}

impl MaybeJsonValue {
    /// Creates a present JSON value result.
    pub const fn found(value: Value) -> Self {
        Self { found: true, value }
    }

    /// Creates a missing JSON value result.
    pub const fn missing() -> Self {
        Self {
            found: false,
            value: Value::Null,
        }
    }

    /// Creates a result from an optional engine value.
    pub fn from_option(value: Option<Value>) -> Self {
        match value {
            Some(value) => Self::found(value),
            None => Self::missing(),
        }
    }

    /// Returns true when the selected JSON value exists.
    pub const fn found_flag(&self) -> bool {
        self.found
    }

    /// Returns true when the selected JSON value exists.
    pub const fn is_found(&self) -> bool {
        self.found
    }

    /// Returns the selected JSON value when it exists.
    pub const fn value(&self) -> Option<&Value> {
        if self.found {
            Some(&self.value)
        } else {
            None
        }
    }

    /// Consumes the result and returns the selected JSON value when it exists.
    pub fn into_option(self) -> Option<Value> {
        if self.found {
            Some(self.value)
        } else {
            None
        }
    }
}

/// JSON versioned point-read result that distinguishes absence from a stored JSON null.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct MaybeJsonVersionedValue {
    found: bool,
    #[serde(default)]
    value: Option<JsonVersionedValue>,
}

impl MaybeJsonVersionedValue {
    /// Creates a present JSON versioned value result.
    pub fn found(value: JsonVersionedValue) -> Self {
        Self {
            found: true,
            value: Some(value),
        }
    }

    /// Creates a missing JSON versioned value result.
    pub const fn missing() -> Self {
        Self {
            found: false,
            value: None,
        }
    }

    /// Creates a result from an optional engine value.
    pub fn from_option(value: Option<JsonVersionedValue>) -> Self {
        match value {
            Some(value) => Self::found(value),
            None => Self::missing(),
        }
    }

    /// Returns true when the selected JSON value exists.
    pub const fn found_flag(&self) -> bool {
        self.found
    }

    /// Returns true when the selected JSON value exists.
    pub const fn is_found(&self) -> bool {
        self.found
    }

    /// Returns the selected JSON value with version metadata when it exists.
    pub const fn value(&self) -> Option<&JsonVersionedValue> {
        if self.found {
            self.value.as_ref()
        } else {
            None
        }
    }

    /// Consumes the result and returns the selected JSON value with version metadata when it exists.
    pub fn into_option(self) -> Option<JsonVersionedValue> {
        if self.found {
            self.value
        } else {
            None
        }
    }
}

/// JSON version-history item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonHistoryItem {
    value: Option<Value>,
    /// The commit version this change was written at — the commit's identity,
    /// distinct from its position in time.
    version: u64,
    /// This change's position on the logical commit timeline: a monotonic
    /// counter assigned per commit, so a fresh database starts small (near 1)
    /// and it is never a calendar date. Pass it to `as_of` to read this exact
    /// point; do not format it as a Unix/epoch timestamp. For when the change
    /// actually happened, use `committed_at`.
    timestamp: u64,
    document_version: Option<u64>,
    tombstone: bool,
    /// The commit's wall-clock instant in microseconds since the Unix epoch
    /// (UTC), or absent when unknown — a commit written before the database
    /// recorded instants, or one whose date the branch cannot vouch for.
    /// Distinct from `timestamp`, which is a commit-timeline position, not a
    /// date.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_at: Option<u64>,
}

impl JsonHistoryItem {
    /// Creates a JSON history item.
    pub fn new(
        value: Option<Value>,
        version: u64,
        timestamp: u64,
        document_version: Option<u64>,
        tombstone: bool,
    ) -> Self {
        Self {
            value,
            version,
            timestamp,
            document_version,
            tombstone,
            committed_at: None,
        }
    }

    /// Returns the commit's wall-clock instant (UTC epoch micros), when known.
    pub const fn committed_at(&self) -> Option<u64> {
        self.committed_at
    }

    /// Attaches the commit's wall-clock instant. A builder so `new`'s call
    /// sites stay put (#3112 S4).
    #[must_use]
    pub fn with_committed_at(mut self, committed_at: Option<u64>) -> Self {
        self.committed_at = committed_at;
        self
    }

    /// Returns the full document value when this row is not a tombstone.
    pub const fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the document version, when present.
    pub const fn document_version(&self) -> Option<u64> {
        self.document_version
    }

    /// Returns true when this item represents a delete.
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

/// Positional JSON batch write/delete result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status, mutation
/// effect, commit receipt, and error; this payload carries only the
/// JSON-specific document version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonBatchItemResult {
    document_version: Option<u64>,
}

impl JsonBatchItemResult {
    /// Creates a JSON batch result payload.
    pub const fn new(document_version: Option<u64>) -> Self {
        Self { document_version }
    }

    /// Returns the document version, when present.
    pub const fn document_version(&self) -> Option<u64> {
        self.document_version
    }
}

/// Positional JSON batch read result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status and error;
/// this payload carries the read facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonBatchGetItemResult {
    found: bool,
    value: Value,
    version: Option<u64>,
    timestamp: Option<u64>,
    document_version: Option<u64>,
}

impl JsonBatchGetItemResult {
    /// Creates a JSON batch read result.
    pub fn new(
        value: Option<Value>,
        version: Option<u64>,
        timestamp: Option<u64>,
        document_version: Option<u64>,
    ) -> Self {
        match value {
            Some(value) => Self {
                found: true,
                value,
                version,
                timestamp,
                document_version,
            },
            None => Self {
                found: false,
                value: Value::Null,
                version,
                timestamp,
                document_version,
            },
        }
    }

    /// Creates a JSON batch read payload for an item that failed validation.
    ///
    /// The failure carries no read facts; the [`BatchItem`](crate::BatchItem)
    /// wrapper carries the error.
    pub const fn not_found() -> Self {
        Self {
            found: false,
            value: Value::Null,
            version: None,
            timestamp: None,
            document_version: None,
        }
    }

    /// Returns true when the selected JSON value exists.
    pub const fn found(&self) -> bool {
        self.found
    }

    /// Returns the selected JSON value, when present.
    pub const fn value(&self) -> Option<&Value> {
        if self.found {
            Some(&self.value)
        } else {
            None
        }
    }

    /// Returns the commit version, when present.
    pub const fn version(&self) -> Option<u64> {
        self.version
    }

    /// Returns the commit timestamp, when present.
    pub const fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    /// Returns the document version, when present.
    pub const fn document_version(&self) -> Option<u64> {
        self.document_version
    }
}

/// Sampled JSON document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonSampleItem {
    key: String,
    value: Value,
    version: u64,
    timestamp: u64,
    document_version: u64,
}

impl JsonSampleItem {
    /// Creates a sampled JSON document.
    pub fn new(
        key: String,
        value: Value,
        version: u64,
        timestamp: u64,
        document_version: u64,
    ) -> Self {
        Self {
            key,
            value,
            version,
            timestamp,
            document_version,
        }
    }

    /// Returns the document key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the full document value.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the document version.
    pub const fn document_version(&self) -> u64 {
        self.document_version
    }
}

/// JSON secondary index definition exposed through the command boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct JsonIndexDefinition {
    name: String,
    space: String,
    field_path: String,
    index_type: JsonIndexType,
    created_version: u64,
    created_timestamp: u64,
}

impl JsonIndexDefinition {
    /// Creates a JSON index definition.
    pub fn new(
        name: String,
        space: String,
        field_path: String,
        index_type: JsonIndexType,
        created_version: u64,
        created_timestamp: u64,
    ) -> Self {
        Self {
            name,
            space,
            field_path,
            index_type,
            created_version,
            created_timestamp,
        }
    }

    /// Returns the index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the product space.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Returns the indexed field path.
    pub fn field_path(&self) -> &str {
        &self.field_path
    }

    /// Returns the index kind.
    pub const fn index_type(&self) -> JsonIndexType {
        self.index_type
    }

    /// Returns the creation commit version.
    pub const fn created_version(&self) -> u64 {
        self.created_version
    }

    /// Returns the creation commit timestamp.
    pub const fn created_timestamp(&self) -> u64 {
        self.created_timestamp
    }
}
