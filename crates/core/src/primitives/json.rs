//! JSON types for JSON primitive
//!
//! This module defines types for the JSON document storage system:
//! - JsonValue: Newtype wrapper around serde_json::Value
//! - JsonPath: Path into a JSON document (e.g., `user.name` or `items[0]`)
//! - PathSegment: Individual path component (Key or Index)
//! - JsonPatch: Patch operation (Set or Delete)
//!
//! # Document Size Limits
//!
//! This module enforces the following limits to prevent memory issues:
//!
//! | Limit | Value | Constant |
//! |-------|-------|----------|
//! | Max document size | 16 MB | [`MAX_DOCUMENT_SIZE`] |
//! | Max nesting depth | 100 levels | [`MAX_NESTING_DEPTH`] |
//! | Max path length | 256 segments | [`MAX_PATH_LENGTH`] |
//! | Max array size | 1M elements | [`MAX_ARRAY_SIZE`] |

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use thiserror::Error;

// =============================================================================
// Document Size Limits
// =============================================================================

/// Maximum document size in bytes (16 MB)
///
/// Documents larger than this will be rejected to prevent memory issues.
/// This limit is checked on create and update operations.
pub const MAX_DOCUMENT_SIZE: usize = 16 * 1024 * 1024; // 16 MB

/// Maximum nesting depth in a JSON document (100 levels)
///
/// Prevents stack overflow during recursive operations like serialization,
/// deserialization, and path traversal.
pub const MAX_NESTING_DEPTH: usize = 100;

/// Maximum path length in segments (256 segments)
///
/// Limits the depth of paths like "a.b.c.d..." to prevent extremely deep
/// nesting and potential performance issues.
pub const MAX_PATH_LENGTH: usize = 256;

/// Maximum array size in elements (1 million elements)
///
/// Prevents creation of extremely large arrays that could cause memory issues.
pub const MAX_ARRAY_SIZE: usize = 1_000_000;

/// Error type for JSON document limit violations
///
/// This error type is specific to JSON document constraints (size, nesting, paths).
/// For general value limits, see [`crate::limits::LimitError`].
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JsonLimitError {
    /// Document exceeds maximum size
    #[error("document size {size} exceeds maximum of {max} bytes")]
    DocumentTooLarge {
        /// Actual document size
        size: usize,
        /// Maximum allowed size
        max: usize,
    },

    /// Document nesting exceeds maximum depth
    #[error("document nesting depth {depth} exceeds maximum of {max} levels")]
    NestingTooDeep {
        /// Actual nesting depth
        depth: usize,
        /// Maximum allowed depth
        max: usize,
    },

    /// Path exceeds maximum length
    #[error("path length {length} exceeds maximum of {max} segments")]
    PathTooLong {
        /// Actual path length
        length: usize,
        /// Maximum allowed length
        max: usize,
    },

    /// Array exceeds maximum size
    #[error("array size {size} exceeds maximum of {max} elements")]
    ArrayTooLarge {
        /// Actual array size
        size: usize,
        /// Maximum allowed size
        max: usize,
    },
}

/// JSON value wrapper
///
/// Newtype around serde_json::Value providing:
/// - Direct access to underlying serde_json::Value via Deref/DerefMut
/// - Easy construction from common types
/// - Serialization/deserialization support
///
/// # Examples
///
/// ```
/// use strata_core::JsonValue;
///
/// // From JSON literals
/// let obj = JsonValue::object();
/// let arr = JsonValue::array();
/// let null = JsonValue::null();
///
/// // From common types
/// let s = JsonValue::from("hello");
/// let n = JsonValue::from(42i64);
/// let b = JsonValue::from(true);
///
/// // Access underlying value
/// assert!(obj.is_object());
/// assert!(arr.is_array());
/// assert!(null.is_null());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct JsonValue(serde_json::Value);

// Compile-time layout assertion: JsonValue must have identical layout to serde_json::Value.
// This guards the two `unsafe` pointer casts in get_at_path() and get_at_path_mut().
const _: () = {
    assert!(std::mem::size_of::<JsonValue>() == std::mem::size_of::<serde_json::Value>());
    assert!(std::mem::align_of::<JsonValue>() == std::mem::align_of::<serde_json::Value>());
};

impl JsonValue {
    /// Create a null JSON value
    pub fn null() -> Self {
        JsonValue(serde_json::Value::Null)
    }

    /// Create an empty JSON object
    pub fn object() -> Self {
        JsonValue(serde_json::Value::Object(serde_json::Map::new()))
    }

    /// Create an empty JSON array
    pub fn array() -> Self {
        JsonValue(serde_json::Value::Array(Vec::new()))
    }

    /// Create from a serde_json::Value
    pub fn from_value(value: serde_json::Value) -> Self {
        JsonValue(value)
    }

    /// Get the underlying serde_json::Value
    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }

    /// Get a reference to the underlying serde_json::Value
    pub fn as_inner(&self) -> &serde_json::Value {
        &self.0
    }

    /// Get a mutable reference to the underlying serde_json::Value
    pub fn as_inner_mut(&mut self) -> &mut serde_json::Value {
        &mut self.0
    }

    /// Serialize to compact JSON string
    pub fn to_json_string(&self) -> String {
        self.0.to_string()
    }

    /// Serialize to pretty JSON string
    pub fn to_json_string_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.0).unwrap_or_else(|_| self.to_json_string())
    }

    /// Calculate approximate size in bytes (for limit checking)
    ///
    /// This is an estimate based on the JSON string representation.
    /// Actual in-memory size may differ.
    pub fn size_bytes(&self) -> usize {
        self.to_json_string().len()
    }

    /// Calculate the maximum nesting depth of this JSON value
    ///
    /// Returns 0 for primitives (null, bool, number, string),
    /// and counts nested objects/arrays.
    pub fn nesting_depth(&self) -> usize {
        fn depth_of(value: &serde_json::Value) -> usize {
            match value {
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => 0,
                serde_json::Value::Array(arr) => 1 + arr.iter().map(depth_of).max().unwrap_or(0),
                serde_json::Value::Object(obj) => 1 + obj.values().map(depth_of).max().unwrap_or(0),
            }
        }
        depth_of(&self.0)
    }

    /// Find the maximum array size in this JSON value (including nested arrays)
    pub fn max_array_size(&self) -> usize {
        fn max_arr_size(value: &serde_json::Value) -> usize {
            match value {
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => 0,
                serde_json::Value::Array(arr) => {
                    let nested_max = arr.iter().map(max_arr_size).max().unwrap_or(0);
                    arr.len().max(nested_max)
                }
                serde_json::Value::Object(obj) => obj.values().map(max_arr_size).max().unwrap_or(0),
            }
        }
        max_arr_size(&self.0)
    }

    /// Validate document size limit
    ///
    /// Returns an error if the document exceeds [`MAX_DOCUMENT_SIZE`].
    pub fn validate_size(&self) -> Result<(), JsonLimitError> {
        let size = self.size_bytes();
        if size > MAX_DOCUMENT_SIZE {
            Err(JsonLimitError::DocumentTooLarge {
                size,
                max: MAX_DOCUMENT_SIZE,
            })
        } else {
            Ok(())
        }
    }

    /// Validate document nesting depth limit
    ///
    /// Returns an error if the document exceeds [`MAX_NESTING_DEPTH`].
    pub fn validate_depth(&self) -> Result<(), JsonLimitError> {
        let depth = self.nesting_depth();
        if depth > MAX_NESTING_DEPTH {
            Err(JsonLimitError::NestingTooDeep {
                depth,
                max: MAX_NESTING_DEPTH,
            })
        } else {
            Ok(())
        }
    }

    /// Validate array size limits
    ///
    /// Returns an error if any array in the document exceeds [`MAX_ARRAY_SIZE`].
    pub fn validate_array_size(&self) -> Result<(), JsonLimitError> {
        let size = self.max_array_size();
        if size > MAX_ARRAY_SIZE {
            Err(JsonLimitError::ArrayTooLarge {
                size,
                max: MAX_ARRAY_SIZE,
            })
        } else {
            Ok(())
        }
    }

    /// Validate all document limits
    ///
    /// Checks size, nesting depth, and array sizes.
    /// Returns the first error encountered, if any.
    pub fn validate(&self) -> Result<(), JsonLimitError> {
        self.validate_size()?;
        self.validate_depth()?;
        self.validate_array_size()?;
        Ok(())
    }
}

// Implement FromStr for parsing from strings
impl FromStr for JsonValue {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map(JsonValue)
    }
}

// Deref to access serde_json::Value methods directly
impl Deref for JsonValue {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for JsonValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Display for easy printing
impl fmt::Display for JsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Default is null
impl Default for JsonValue {
    fn default() -> Self {
        Self::null()
    }
}

// From implementations for common types
impl From<serde_json::Value> for JsonValue {
    fn from(v: serde_json::Value) -> Self {
        JsonValue(v)
    }
}

impl From<JsonValue> for serde_json::Value {
    fn from(v: JsonValue) -> Self {
        v.0
    }
}

impl From<bool> for JsonValue {
    fn from(v: bool) -> Self {
        JsonValue(serde_json::Value::Bool(v))
    }
}

impl From<i64> for JsonValue {
    fn from(v: i64) -> Self {
        JsonValue(serde_json::Value::Number(v.into()))
    }
}

impl From<i32> for JsonValue {
    fn from(v: i32) -> Self {
        JsonValue(serde_json::Value::Number(v.into()))
    }
}

impl From<u64> for JsonValue {
    fn from(v: u64) -> Self {
        JsonValue(serde_json::Value::Number(v.into()))
    }
}

impl From<u32> for JsonValue {
    fn from(v: u32) -> Self {
        JsonValue(serde_json::Value::Number(v.into()))
    }
}

impl From<f64> for JsonValue {
    fn from(v: f64) -> Self {
        JsonValue(
            serde_json::Number::from_f64(v)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
        )
    }
}

impl From<&str> for JsonValue {
    fn from(v: &str) -> Self {
        JsonValue(serde_json::Value::String(v.to_string()))
    }
}

impl From<String> for JsonValue {
    fn from(v: String) -> Self {
        JsonValue(serde_json::Value::String(v))
    }
}

impl<T: Into<JsonValue>> From<Vec<T>> for JsonValue {
    fn from(v: Vec<T>) -> Self {
        JsonValue(serde_json::Value::Array(
            v.into_iter().map(|x| x.into().0).collect(),
        ))
    }
}

impl<T: Into<JsonValue>> From<Option<T>> for JsonValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => JsonValue::null(),
        }
    }
}

// =============================================================================
// JsonPath and PathSegment
// =============================================================================

/// Error type for JSON path parsing
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PathParseError {
    /// Empty key in path
    #[error("empty key in path at position {0}")]
    EmptyKey(usize),
    /// Unclosed bracket
    #[error("unclosed bracket starting at position {0}")]
    UnclosedBracket(usize),
    /// Invalid array index
    #[error("invalid array index at position {0}: {1}")]
    InvalidIndex(usize, String),
    /// Unexpected character
    #[error("unexpected character '{0}' at position {1}")]
    UnexpectedChar(char, usize),
}

/// A segment in a JSON path
///
/// Paths are composed of key segments (object property access)
/// and index segments (array element access).
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::PathSegment;
///
/// let key = PathSegment::Key("name".to_string());
/// let idx = PathSegment::Index(0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PathSegment {
    /// Object key: `.foo`
    Key(String),
    /// Array index: `[0]`
    Index(usize),
}

impl fmt::Display for PathSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathSegment::Key(k) => write!(f, ".{}", k),
            PathSegment::Index(i) => write!(f, "[{}]", i),
        }
    }
}

/// A path into a JSON document
///
/// JsonPath represents a location within a JSON document using a sequence
/// of key and index segments. Paths support:
///
/// - Object property access: `.foo`
/// - Array index access: `[0]`
/// - Nested paths: `.user.address.city` or `.items[0].name`
///
/// # Path Syntax (Subset)
///
/// | Syntax | Meaning | Example |
/// |--------|---------|---------|
/// | `.key` | Object property | `.user` |
/// | `[n]` | Array index | `[0]` |
/// | `.key1.key2` | Nested property | `.user.name` |
/// | `.key[n]` | Property then index | `.items[0]` |
/// | (empty) | Root | `` |
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::JsonPath;
///
/// // Create paths
/// let root = JsonPath::root();
/// let user_name = JsonPath::root().key("user").key("name");
/// let first_item = JsonPath::root().key("items").index(0);
///
/// // Parse from string
/// let path: JsonPath = "user.name".parse().unwrap();
/// assert_eq!(path, user_name);
///
/// // Check relationships
/// let user = JsonPath::root().key("user");
/// assert!(user.is_ancestor_of(&user_name));
/// assert!(user_name.is_descendant_of(&user));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct JsonPath {
    segments: Vec<PathSegment>,
}

impl JsonPath {
    /// Create the root path (empty path)
    pub fn root() -> Self {
        JsonPath {
            segments: Vec::new(),
        }
    }

    /// Create a path from a vector of segments
    pub fn from_segments(segments: Vec<PathSegment>) -> Self {
        JsonPath { segments }
    }

    /// Get the path segments
    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }

    /// Get the number of segments in the path
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Check if this is the root path (empty)
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Check if this is the root path
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    /// Append a key segment (builder pattern)
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.segments.push(PathSegment::Key(key.into()));
        self
    }

    /// Append an index segment (builder pattern)
    pub fn index(mut self, idx: usize) -> Self {
        self.segments.push(PathSegment::Index(idx));
        self
    }

    /// Push a key segment (mutating)
    pub fn push_key(&mut self, key: impl Into<String>) {
        self.segments.push(PathSegment::Key(key.into()));
    }

    /// Push an index segment (mutating)
    pub fn push_index(&mut self, idx: usize) {
        self.segments.push(PathSegment::Index(idx));
    }

    /// Get the parent path (None if root)
    pub fn parent(&self) -> Option<JsonPath> {
        if self.segments.is_empty() {
            None
        } else {
            let mut parent = self.clone();
            parent.segments.pop();
            Some(parent)
        }
    }

    /// Get the last segment (None if root)
    pub fn last_segment(&self) -> Option<&PathSegment> {
        self.segments.last()
    }

    /// Check if this path is an ancestor of another (or equal)
    ///
    /// A path is an ancestor if it is a prefix of the other path.
    /// The root path is an ancestor of all paths.
    /// A path is considered an ancestor of itself.
    pub fn is_ancestor_of(&self, other: &JsonPath) -> bool {
        if self.segments.len() > other.segments.len() {
            return false;
        }
        self.segments
            .iter()
            .zip(other.segments.iter())
            .all(|(a, b)| a == b)
    }

    /// Check if this path is a descendant of another (or equal)
    ///
    /// A path is a descendant if the other path is a prefix of this path.
    /// All paths are descendants of the root path.
    /// A path is considered a descendant of itself.
    pub fn is_descendant_of(&self, other: &JsonPath) -> bool {
        other.is_ancestor_of(self)
    }

    /// Check if this path is a strict ancestor of another (not equal)
    ///
    /// A path is a strict ancestor if it is a proper prefix of the other path.
    /// The root path is a strict ancestor of all non-root paths.
    pub fn is_strict_ancestor_of(&self, other: &JsonPath) -> bool {
        self.segments.len() < other.segments.len() && self.is_ancestor_of(other)
    }

    /// Check if this path is a strict descendant of another (not equal)
    ///
    /// A path is a strict descendant if the other path is a proper prefix of this path.
    /// All non-root paths are strict descendants of the root path.
    pub fn is_strict_descendant_of(&self, other: &JsonPath) -> bool {
        other.is_strict_ancestor_of(self)
    }

    /// Find the common ancestor of two paths
    ///
    /// Returns the longest common prefix of both paths.
    /// If the paths share no common prefix, returns the root path.
    pub fn common_ancestor(&self, other: &JsonPath) -> JsonPath {
        let mut common_segments = Vec::new();
        for (a, b) in self.segments.iter().zip(other.segments.iter()) {
            if a == b {
                common_segments.push(a.clone());
            } else {
                break;
            }
        }
        JsonPath::from_segments(common_segments)
    }

    /// Check if this path would be affected by a write at the given path
    ///
    /// A path is affected if:
    /// - The write path is an ancestor (write affects this path and all descendants)
    /// - The write path equals this path (direct modification)
    /// - The write path is a descendant (partial modification of this path's subtree)
    ///
    /// This is equivalent to `overlaps()` but with clearer semantics for conflict detection.
    pub fn is_affected_by(&self, write_path: &JsonPath) -> bool {
        self.overlaps(write_path)
    }

    /// Check if two paths overlap (one is ancestor/descendant of the other)
    ///
    /// Used for conflict detection: if two paths overlap and both are
    /// accessed in a transaction (one read, one write), there's a potential conflict.
    pub fn overlaps(&self, other: &JsonPath) -> bool {
        self.is_ancestor_of(other) || self.is_descendant_of(other)
    }

    /// Validate path length limit
    ///
    /// Returns an error if the path exceeds [`MAX_PATH_LENGTH`].
    pub fn validate(&self) -> Result<(), JsonLimitError> {
        let length = self.segments.len();
        if length > MAX_PATH_LENGTH {
            Err(JsonLimitError::PathTooLong {
                length,
                max: MAX_PATH_LENGTH,
            })
        } else {
            Ok(())
        }
    }

    /// Convert to a string representation
    pub fn to_path_string(&self) -> String {
        if self.segments.is_empty() {
            return String::new();
        }
        let mut result = String::new();
        for seg in &self.segments {
            match seg {
                PathSegment::Key(k) => {
                    if !result.is_empty() {
                        result.push('.');
                    }
                    result.push_str(k);
                }
                PathSegment::Index(i) => {
                    result.push('[');
                    result.push_str(&i.to_string());
                    result.push(']');
                }
            }
        }
        result
    }
}

impl FromStr for JsonPath {
    type Err = PathParseError;

    /// Parse a path from a string
    ///
    /// Supported syntax:
    /// - `foo` or `.foo` - object key
    /// - `[0]` - array index
    /// - `foo.bar` - nested keys
    /// - `foo[0]` - key then index
    /// - `foo[0].bar` - mixed
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(JsonPath::root());
        }

        let mut segments = Vec::new();
        let chars: Vec<char> = s.chars().collect();
        let mut i = 0;

        // Skip leading dot if present
        if i < chars.len() && chars[i] == '.' {
            i += 1;
        }

        while i < chars.len() {
            let c = chars[i];

            if c == '.' {
                // Start of a key segment
                i += 1;
                if i >= chars.len() {
                    return Err(PathParseError::EmptyKey(i));
                }
            }

            if chars[i] == '[' {
                // Array index segment
                let start = i;
                i += 1;
                let idx_start = i;

                // Find closing bracket
                while i < chars.len() && chars[i] != ']' {
                    i += 1;
                }

                if i >= chars.len() {
                    return Err(PathParseError::UnclosedBracket(start));
                }

                let idx_str: String = chars[idx_start..i].iter().collect();
                let idx = idx_str
                    .parse::<usize>()
                    .map_err(|_| PathParseError::InvalidIndex(idx_start, idx_str))?;

                segments.push(PathSegment::Index(idx));
                i += 1; // Skip closing bracket
            } else if chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-' {
                // Key segment
                let key_start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-')
                {
                    i += 1;
                }
                let key: String = chars[key_start..i].iter().collect();
                segments.push(PathSegment::Key(key));
            } else {
                return Err(PathParseError::UnexpectedChar(chars[i], i));
            }
        }

        Ok(JsonPath { segments })
    }
}

impl fmt::Display for JsonPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path_string())
    }
}

// =============================================================================
// JsonPatch
// =============================================================================

/// A patch operation on a JSON document
///
/// JsonPatch represents an atomic mutation to a JSON document.
/// Patches are used for:
/// - WAL recording (patch-based persistence)
/// - Transaction tracking
/// - Conflict detection
///
/// # Subset (Not Full RFC 6902)
///
/// **Important**: This is a minimal subset of JSON Patch operations, NOT full
/// RFC 6902 compliance. This implements only the operations needed for path-based
/// mutation semantics.
///
/// ## Supported Operations
///
/// - [`Set`](JsonPatch::Set): Replace/create value at path (similar to RFC 6902 `replace`)
/// - [`Delete`](JsonPatch::Delete): Remove value at path (RFC 6902 `remove`)
///
/// ## NOT Supported (RFC 6902 operations)
///
/// The following RFC 6902 operations are **not implemented** currently:
///
/// - `add`: Insert at path (differs from `replace` for arrays and missing keys)
/// - `test`: Conditional patch execution (verify value before applying)
/// - `move`: Move value from one path to another
/// - `copy`: Copy value from one path to another
///
/// These operations are reserved for future enhancements if needed. The WAL entry
/// type 0x24 (`JsonPatch`) is reserved for future RFC 6902 support.
///
/// ## Design Rationale
///
/// The implementation prioritizes **semantic lock-in** over feature completeness. The `Set` and `Delete`
/// operations cover the core mutation semantics needed for path-based JSON documents.
/// Complex transformations (move, copy) can be composed from these primitives via
/// read-modify-write patterns.
///
/// # Conflict Detection
///
/// Two patches conflict if their paths overlap (one is ancestor/descendant of the other).
/// This is used for region-based conflict detection in transactions.
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonPatch, JsonPath, JsonValue};
///
/// // Create patches
/// let set = JsonPatch::set("user.name", JsonValue::from("Alice"));
/// let delete = JsonPatch::delete("user.email");
///
/// // Check conflict
/// let patch1 = JsonPatch::set("user", JsonValue::object());
/// let patch2 = JsonPatch::set("user.name", JsonValue::from("Bob"));
/// assert!(patch1.conflicts_with(&patch2)); // user is ancestor of user.name
///
/// // Non-conflicting patches
/// let patch3 = JsonPatch::set("user.name", JsonValue::from("Alice"));
/// let patch4 = JsonPatch::set("user.email", JsonValue::from("alice@example.com"));
/// assert!(!patch3.conflicts_with(&patch4)); // Different paths
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JsonPatch {
    /// Set value at path
    Set {
        /// The path to set
        path: JsonPath,
        /// The value to set
        value: JsonValue,
    },
    /// Delete value at path
    Delete {
        /// The path to delete
        path: JsonPath,
    },
}

impl JsonPatch {
    /// Create a Set patch
    ///
    /// Convenience constructor that parses the path from a string.
    ///
    /// # Panics
    ///
    /// Panics if the path string is invalid.
    pub fn set(path: impl AsRef<str>, value: JsonValue) -> Self {
        JsonPatch::Set {
            path: path
                .as_ref()
                .parse()
                .expect("Invalid path in JsonPatch::set"),
            value,
        }
    }

    /// Create a Set patch with a pre-parsed path
    pub fn set_at(path: JsonPath, value: JsonValue) -> Self {
        JsonPatch::Set { path, value }
    }

    /// Create a Delete patch
    ///
    /// Convenience constructor that parses the path from a string.
    ///
    /// # Panics
    ///
    /// Panics if the path string is invalid.
    pub fn delete(path: impl AsRef<str>) -> Self {
        JsonPatch::Delete {
            path: path
                .as_ref()
                .parse()
                .expect("Invalid path in JsonPatch::delete"),
        }
    }

    /// Create a Delete patch with a pre-parsed path
    pub fn delete_at(path: JsonPath) -> Self {
        JsonPatch::Delete { path }
    }

    /// Get the path affected by this patch
    pub fn path(&self) -> &JsonPath {
        match self {
            JsonPatch::Set { path, .. } => path,
            JsonPatch::Delete { path } => path,
        }
    }

    /// Check if this patch conflicts with another
    ///
    /// Two patches conflict if their paths overlap (one is ancestor/descendant of the other).
    /// This is used for region-based conflict detection.
    ///
    /// Note: Two Set patches to the same path also conflict, but can sometimes be
    /// resolved by last-writer-wins semantics depending on the use case.
    pub fn conflicts_with(&self, other: &JsonPatch) -> bool {
        self.path().overlaps(other.path())
    }

    /// Check if this is a Set operation
    pub fn is_set(&self) -> bool {
        matches!(self, JsonPatch::Set { .. })
    }

    /// Check if this is a Delete operation
    pub fn is_delete(&self) -> bool {
        matches!(self, JsonPatch::Delete { .. })
    }

    /// Get the value if this is a Set patch
    pub fn value(&self) -> Option<&JsonValue> {
        match self {
            JsonPatch::Set { value, .. } => Some(value),
            JsonPatch::Delete { .. } => None,
        }
    }
}

impl fmt::Display for JsonPatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonPatch::Set { path, value } => write!(f, "SET {} = {}", path, value),
            JsonPatch::Delete { path } => write!(f, "DELETE {}", path),
        }
    }
}

// =============================================================================
// Path Operations Error
// =============================================================================

/// Error type for path operations
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JsonPathError {
    /// Type mismatch during path traversal
    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        /// Expected type
        expected: &'static str,
        /// Actual type found
        found: &'static str,
    },

    /// Array index out of bounds
    #[error("index out of bounds: {index} >= {len}")]
    IndexOutOfBounds {
        /// The requested index
        index: usize,
        /// The array length
        len: usize,
    },

    /// Path not found
    #[error("path not found")]
    NotFound,
}

// =============================================================================
// Path Operations
// =============================================================================

/// Get value at path within a JSON document
///
/// Traverses the document following the path segments, returning a reference
/// to the value at the specified location.
///
/// # Arguments
///
/// * `value` - The root JSON value to traverse
/// * `path` - The path to the desired value
///
/// # Returns
///
/// * `Some(&JsonValue)` - Reference to the value at the path
/// * `None` - If the path doesn't exist or there's a type mismatch
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, JsonPath, get_at_path};
///
/// let json: JsonValue = serde_json::json!({
///     "user": {
///         "name": "Alice",
///         "scores": [100, 95, 88]
///     }
/// }).into();
///
/// // Get nested object property
/// let path: JsonPath = "user.name".parse().unwrap();
/// let name = get_at_path(&json, &path).unwrap();
/// assert_eq!(name.as_str(), Some("Alice"));
///
/// // Get array element
/// let path: JsonPath = "user.scores[1]".parse().unwrap();
/// let score = get_at_path(&json, &path).unwrap();
/// assert_eq!(score.as_i64(), Some(95));
///
/// // Root path returns the entire document
/// assert_eq!(get_at_path(&json, &JsonPath::root()), Some(&json));
/// ```
pub fn get_at_path<'a>(value: &'a JsonValue, path: &JsonPath) -> Option<&'a JsonValue> {
    if path.is_root() {
        return Some(value);
    }

    let mut current: &serde_json::Value = value.as_inner();

    for segment in path.segments() {
        match (segment, current) {
            (PathSegment::Key(key), serde_json::Value::Object(obj)) => {
                current = obj.get(key)?;
            }
            (PathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                current = arr.get(*idx)?;
            }
            _ => return None,
        }
    }

    // SAFETY: This transmute is safe because:
    // 1. JsonValue has #[repr(transparent)], guaranteeing identical memory layout to serde_json::Value
    // 2. The returned reference's lifetime is tied to the input JsonValue reference
    // 3. Both types have the same alignment requirements
    Some(unsafe { &*(current as *const serde_json::Value as *const JsonValue) })
}

/// Get mutable reference to value at path within a JSON document
///
/// Traverses the document following the path segments, returning a mutable
/// reference to the value at the specified location.
///
/// # Arguments
///
/// * `value` - The root JSON value to traverse
/// * `path` - The path to the desired value
///
/// # Returns
///
/// * `Some(&mut JsonValue)` - Mutable reference to the value at the path
/// * `None` - If the path doesn't exist or there's a type mismatch
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, JsonPath, get_at_path_mut};
///
/// let mut json: JsonValue = serde_json::json!({
///     "user": {
///         "name": "Alice"
///     }
/// }).into();
///
/// // Modify nested value
/// let path: JsonPath = "user.name".parse().unwrap();
/// if let Some(name) = get_at_path_mut(&mut json, &path) {
///     *name = JsonValue::from("Bob");
/// }
///
/// // Verify the change
/// use strata_core::primitives::json::get_at_path;
/// let name = get_at_path(&json, &path).unwrap();
/// assert_eq!(name.as_str(), Some("Bob"));
/// ```
pub fn get_at_path_mut<'a>(value: &'a mut JsonValue, path: &JsonPath) -> Option<&'a mut JsonValue> {
    if path.is_root() {
        return Some(value);
    }

    let inner: &mut serde_json::Value = value.as_inner_mut();
    let mut current: &mut serde_json::Value = inner;

    for segment in path.segments() {
        current = match (segment, current) {
            (PathSegment::Key(key), serde_json::Value::Object(obj)) => obj.get_mut(key)?,
            (PathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get_mut(*idx)?,
            _ => return None,
        };
    }

    // SAFETY: This transmute is safe because:
    // 1. JsonValue has #[repr(transparent)], guaranteeing identical memory layout to serde_json::Value
    // 2. The returned reference's lifetime is tied to the input JsonValue reference
    // 3. Both types have the same alignment requirements
    Some(unsafe { &mut *(current as *mut serde_json::Value as *mut JsonValue) })
}

// =============================================================================
// Path Mutation
// =============================================================================

/// Set value at path within a JSON document
///
/// Creates intermediate objects and arrays as needed when the path doesn't exist.
/// The type of intermediate container (object vs array) is determined by the
/// next segment in the path.
///
/// # Arguments
///
/// * `root` - The root JSON value to modify
/// * `path` - The path where the value should be set
/// * `value` - The value to set at the path
///
/// # Returns
///
/// * `Ok(())` - Value was set successfully
/// * `Err(JsonPathError)` - Type mismatch or index out of bounds
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, JsonPath, set_at_path, get_at_path};
///
/// // Set value at nested path, creating intermediate objects
/// let mut json = JsonValue::object();
/// let path: JsonPath = "user.profile.name".parse().unwrap();
/// set_at_path(&mut json, &path, JsonValue::from("Alice")).unwrap();
///
/// // Verify the value was set
/// assert_eq!(
///     get_at_path(&json, &path).unwrap().as_str(),
///     Some("Alice")
/// );
///
/// // Replace root value
/// let mut value = JsonValue::from(42);
/// set_at_path(&mut value, &JsonPath::root(), JsonValue::from(100)).unwrap();
/// assert_eq!(value.as_i64(), Some(100));
/// ```
pub fn set_at_path(
    root: &mut JsonValue,
    path: &JsonPath,
    value: JsonValue,
) -> Result<(), JsonPathError> {
    // Handle root path - replace entire value
    if path.is_root() {
        *root = value;
        return Ok(());
    }

    let segments = path.segments();
    if segments.is_empty() {
        *root = value;
        return Ok(());
    }

    let inner = root.as_inner_mut();

    // Navigate to parent, creating intermediate containers as needed
    let (parent_segments, last_segment) = segments.split_at(segments.len() - 1);
    let last_segment = &last_segment[0];

    let mut current = inner;

    // Navigate to parent, creating intermediates
    for (i, segment) in parent_segments.iter().enumerate() {
        let next_segment = &segments[i + 1];

        match segment {
            PathSegment::Key(key) => {
                if !current.is_object() {
                    return Err(JsonPathError::TypeMismatch {
                        expected: "object",
                        found: value_type_name(current),
                    });
                }
                let obj = current.as_object_mut().unwrap();
                if !obj.contains_key(key) {
                    let new_container = match next_segment {
                        PathSegment::Key(_) => serde_json::Value::Object(serde_json::Map::new()),
                        PathSegment::Index(_) => serde_json::Value::Array(Vec::new()),
                    };
                    obj.insert(key.clone(), new_container);
                }
                current = obj.get_mut(key).unwrap();
            }
            PathSegment::Index(idx) => {
                if !current.is_array() {
                    return Err(JsonPathError::TypeMismatch {
                        expected: "array",
                        found: value_type_name(current),
                    });
                }
                let arr = current.as_array_mut().unwrap();
                if *idx >= arr.len() {
                    return Err(JsonPathError::IndexOutOfBounds {
                        index: *idx,
                        len: arr.len(),
                    });
                }
                current = &mut arr[*idx];
            }
        }
    }

    // Set the value at the last segment
    match last_segment {
        PathSegment::Key(key) => {
            if !current.is_object() {
                return Err(JsonPathError::TypeMismatch {
                    expected: "object",
                    found: value_type_name(current),
                });
            }
            let obj = current.as_object_mut().unwrap();
            obj.insert(key.clone(), value.into_inner());
            Ok(())
        }
        PathSegment::Index(idx) => {
            if !current.is_array() {
                return Err(JsonPathError::TypeMismatch {
                    expected: "array",
                    found: value_type_name(current),
                });
            }
            let arr = current.as_array_mut().unwrap();
            if *idx < arr.len() {
                arr[*idx] = value.into_inner();
                Ok(())
            } else if *idx == arr.len() {
                arr.push(value.into_inner());
                Ok(())
            } else {
                Err(JsonPathError::IndexOutOfBounds {
                    index: *idx,
                    len: arr.len(),
                })
            }
        }
    }
}

/// Helper to get type name for error messages
fn value_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// =============================================================================
// Path Deletion
// =============================================================================

/// Delete value at path within a JSON document
///
/// Removes the value at the specified path. For objects, this removes the key.
/// For arrays, this removes the element and shifts subsequent elements.
/// Deleting the root replaces the value with null.
///
/// # Arguments
///
/// * `root` - The root JSON value to modify
/// * `path` - The path to delete
///
/// # Returns
///
/// * `Ok(Some(value))` - The deleted value
/// * `Ok(None)` - The path didn't exist
/// * `Err(JsonPathError)` - Type mismatch during traversal
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, JsonPath, delete_at_path, get_at_path};
///
/// // Delete object key
/// let mut json: JsonValue = r#"{"name": "Alice", "age": 30}"#.parse().unwrap();
/// let deleted = delete_at_path(&mut json, &"name".parse().unwrap()).unwrap();
/// assert_eq!(deleted.unwrap().as_str(), Some("Alice"));
/// assert!(get_at_path(&json, &"name".parse().unwrap()).is_none());
///
/// // Delete array element (shifts indices)
/// let mut arr: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
/// delete_at_path(&mut arr, &"[1]".parse().unwrap()).unwrap();
/// assert_eq!(arr.as_array().unwrap().len(), 2);
/// assert_eq!(arr.as_array().unwrap()[1].as_i64(), Some(3)); // Was at index 2
/// ```
pub fn delete_at_path(
    root: &mut JsonValue,
    path: &JsonPath,
) -> Result<Option<JsonValue>, JsonPathError> {
    // Handle root path - replace with null
    if path.is_root() {
        let old = std::mem::take(root);
        return Ok(Some(old));
    }

    let segments = path.segments();
    if segments.is_empty() {
        let old = std::mem::take(root);
        return Ok(Some(old));
    }

    // Navigate to parent
    let parent_path = path.parent().ok_or(JsonPathError::NotFound)?;
    let parent = get_at_path_mut(root, &parent_path).ok_or(JsonPathError::NotFound)?;
    let parent_inner = parent.as_inner_mut();

    // Delete at the last segment
    let last_segment = segments.last().unwrap();
    match last_segment {
        PathSegment::Key(key) => {
            if !parent_inner.is_object() {
                return Err(JsonPathError::TypeMismatch {
                    expected: "object",
                    found: value_type_name(parent_inner),
                });
            }
            let obj = parent_inner.as_object_mut().unwrap();
            let removed = obj.remove(key);
            Ok(removed.map(JsonValue::from_value))
        }
        PathSegment::Index(idx) => {
            if !parent_inner.is_array() {
                return Err(JsonPathError::TypeMismatch {
                    expected: "array",
                    found: value_type_name(parent_inner),
                });
            }
            let arr = parent_inner.as_array_mut().unwrap();
            if *idx < arr.len() {
                let removed = arr.remove(*idx);
                Ok(Some(JsonValue::from_value(removed)))
            } else {
                Ok(None)
            }
        }
    }
}

// =============================================================================
// Patch Application
// =============================================================================

/// Apply multiple patches to a JSON document
///
/// Applies each patch in order. If any patch fails, the document may be
/// left in a partially modified state (patches are not atomic).
///
/// # Arguments
///
/// * `root` - The root JSON value to modify
/// * `patches` - The patches to apply in order
///
/// # Returns
///
/// * `Ok(())` - All patches applied successfully
/// * `Err(JsonPathError)` - A patch failed to apply
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, JsonPath, JsonPatch, apply_patches, get_at_path};
///
/// let mut json = JsonValue::object();
///
/// let patches = vec![
///     JsonPatch::set("name", JsonValue::from("Alice")),
///     JsonPatch::set("age", JsonValue::from(30i64)),
///     JsonPatch::set("tags", JsonValue::array()),
/// ];
///
/// apply_patches(&mut json, &patches).unwrap();
///
/// assert_eq!(get_at_path(&json, &"name".parse().unwrap()).unwrap().as_str(), Some("Alice"));
/// assert_eq!(get_at_path(&json, &"age".parse().unwrap()).unwrap().as_i64(), Some(30));
/// ```
pub fn apply_patches(root: &mut JsonValue, patches: &[JsonPatch]) -> Result<(), JsonPathError> {
    for patch in patches {
        match patch {
            JsonPatch::Set { path, value } => {
                set_at_path(root, path, value.clone())?;
            }
            JsonPatch::Delete { path } => {
                delete_at_path(root, path)?;
            }
        }
    }
    Ok(())
}

// =============================================================================
// RFC 7396 JSON Merge Patch
// =============================================================================

/// Apply RFC 7396 JSON Merge Patch
///
/// Merges `patch` into `target` following RFC 7396 semantics:
/// - If patch is an object, recursively merge each key
/// - If a key's value is null, remove that key from target
/// - If a key exists in both, recursively merge (if objects) or replace (otherwise)
/// - If a key only exists in patch, add it to target
/// - If patch is not an object, it replaces target entirely
///
/// # Arguments
///
/// * `target` - The target JSON value to modify in place
/// * `patch` - The patch to apply
///
/// # Examples
///
/// ```
/// use strata_core::primitives::json::{JsonValue, merge_patch};
///
/// // Merge objects
/// let mut target: JsonValue = serde_json::json!({"a": 1, "b": 2}).into();
/// let patch: JsonValue = serde_json::json!({"b": 3, "c": 4}).into();
/// merge_patch(&mut target, &patch);
/// // target is now {"a": 1, "b": 3, "c": 4}
///
/// // Null removes keys
/// let mut target: JsonValue = serde_json::json!({"a": 1, "b": 2}).into();
/// let patch: JsonValue = serde_json::json!({"b": null}).into();
/// merge_patch(&mut target, &patch);
/// // target is now {"a": 1}
///
/// // Non-object patch replaces entirely
/// let mut target: JsonValue = serde_json::json!({"a": 1}).into();
/// let patch: JsonValue = serde_json::json!([1, 2, 3]).into();
/// merge_patch(&mut target, &patch);
/// // target is now [1, 2, 3]
/// ```
pub fn merge_patch(target: &mut JsonValue, patch: &JsonValue) {
    merge_patch_inner(target.as_inner_mut(), patch.as_inner());
}

/// Internal implementation operating on serde_json::Value
fn merge_patch_inner(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if let serde_json::Value::Object(patch_obj) = patch {
        // Patch is an object - merge recursively
        if !target.is_object() {
            *target = serde_json::Value::Object(serde_json::Map::new());
        }
        if let serde_json::Value::Object(target_obj) = target {
            for (key, value) in patch_obj {
                if value.is_null() {
                    // Null removes the key
                    target_obj.remove(key);
                } else if let Some(target_value) = target_obj.get_mut(key) {
                    // Key exists - recursively merge
                    merge_patch_inner(target_value, value);
                } else {
                    // Key doesn't exist - add it
                    target_obj.insert(key.clone(), value.clone());
                }
            }
        }
    } else {
        // Patch is not an object - replace target entirely
        *target = patch.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_value_null() {
        let v = JsonValue::null();
        assert!(v.is_null());
    }

    #[test]
    fn test_json_value_object() {
        let v = JsonValue::object();
        assert!(v.is_object());
        assert_eq!(v.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_json_value_array() {
        let v = JsonValue::array();
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_json_value_from_bool() {
        let t = JsonValue::from(true);
        let f = JsonValue::from(false);
        assert_eq!(t.as_bool(), Some(true));
        assert_eq!(f.as_bool(), Some(false));
    }

    #[test]
    fn test_json_value_from_i64() {
        let v = JsonValue::from(42i64);
        assert_eq!(v.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_from_i32() {
        let v = JsonValue::from(42i32);
        assert_eq!(v.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_from_u64() {
        let v = JsonValue::from(42u64);
        assert_eq!(v.as_u64(), Some(42));
    }

    #[test]
    fn test_json_value_from_f64() {
        let v = JsonValue::from(3.14f64);
        assert!((v.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_json_value_from_str_ref() {
        let v = JsonValue::from("hello");
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_json_value_from_string() {
        let v = JsonValue::from("world".to_string());
        assert_eq!(v.as_str(), Some("world"));
    }

    #[test]
    fn test_json_value_from_vec() {
        let v: JsonValue = vec![1i64, 2, 3].into();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64(), Some(1));
    }

    #[test]
    fn test_json_value_from_option_some() {
        let v: JsonValue = Some(42i64).into();
        assert_eq!(v.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_from_option_none() {
        let v: JsonValue = Option::<i64>::None.into();
        assert!(v.is_null());
    }

    #[test]
    fn test_json_value_deref() {
        let v = JsonValue::from(42i64);
        // Access serde_json::Value methods via Deref
        assert!(v.is_number());
        assert!(!v.is_string());
    }

    #[test]
    fn test_json_value_deref_mut() {
        let mut v = JsonValue::object();
        // Mutate via DerefMut
        v.as_object_mut()
            .unwrap()
            .insert("key".to_string(), serde_json::json!(123));
        assert_eq!(v["key"].as_i64(), Some(123));
    }

    #[test]
    fn test_json_value_parse() {
        let v: JsonValue = r#"{"name": "test", "value": 42}"#.parse().unwrap();
        assert!(v.is_object());
        assert_eq!(v["name"].as_str(), Some("test"));
        assert_eq!(v["value"].as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_parse_invalid() {
        let result: Result<JsonValue, _> = "not valid json {".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_value_to_json_string() {
        let v: JsonValue = r#"{"a":1}"#.parse().unwrap();
        let s = v.to_json_string();
        assert!(s.contains("\"a\""));
        assert!(s.contains("1"));
    }

    #[test]
    fn test_json_value_display() {
        let v = JsonValue::from(42i64);
        let s = format!("{}", v);
        assert_eq!(s, "42");
    }

    #[test]
    fn test_json_value_default() {
        let v = JsonValue::default();
        assert!(v.is_null());
    }

    #[test]
    fn test_json_value_clone() {
        let v1 = JsonValue::from("test");
        let v2 = v1.clone();
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_json_value_equality() {
        let v1 = JsonValue::from(42i64);
        let v2 = JsonValue::from(42i64);
        let v3 = JsonValue::from(43i64);
        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_json_value_serialization() {
        let v: JsonValue = r#"{"key": "value"}"#.parse().unwrap();
        let json = serde_json::to_string(&v).unwrap();
        let v2: JsonValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_json_value_into_inner() {
        let v = JsonValue::from(42i64);
        let inner: serde_json::Value = v.into_inner();
        assert_eq!(inner.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_as_inner() {
        let v = JsonValue::from(42i64);
        let inner: &serde_json::Value = v.as_inner();
        assert_eq!(inner.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_size_bytes() {
        let v: JsonValue = r#"{"key": "value"}"#.parse().unwrap();
        let size = v.size_bytes();
        // Should be at least the length of the JSON string
        assert!(size > 0);
        assert!(size <= 20); // Reasonable upper bound for this small object
    }

    #[test]
    fn test_json_value_from_serde_json_value() {
        let serde_val = serde_json::json!({"nested": {"deep": true}});
        let v = JsonValue::from(serde_val);
        assert!(v.is_object());
        assert!(v["nested"]["deep"].as_bool().unwrap());
    }

    #[test]
    fn test_json_value_into_serde_json_value() {
        let v = JsonValue::from(42i64);
        let serde_val: serde_json::Value = v.into();
        assert_eq!(serde_val.as_i64(), Some(42));
    }

    #[test]
    fn test_json_value_f64_nan() {
        // NaN/Infinity cannot be represented in JSON, should become null
        let v = JsonValue::from(f64::NAN);
        assert!(v.is_null());
    }

    #[test]
    fn test_json_value_f64_infinity() {
        // Infinity cannot be represented in JSON, should become null
        let v = JsonValue::from(f64::INFINITY);
        assert!(v.is_null());
    }

    #[test]
    fn test_json_value_nested_modification() {
        let mut v: JsonValue = r#"{"user": {"name": "Alice"}}"#.parse().unwrap();
        v["user"]["name"] = serde_json::json!("Bob");
        assert_eq!(v["user"]["name"].as_str(), Some("Bob"));
    }

    #[test]
    fn test_json_value_to_json_string_pretty() {
        let v: JsonValue = r#"{"a":1,"b":2}"#.parse().unwrap();
        let pretty = v.to_json_string_pretty();
        // Pretty output should have newlines
        assert!(pretty.contains('\n'));
    }

    #[test]
    fn test_json_value_from_value() {
        let serde_val = serde_json::json!([1, 2, 3]);
        let v = JsonValue::from_value(serde_val);
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 3);
    }

    // ========================================
    // JsonPath Tests
    // ========================================

    #[test]
    fn test_path_root() {
        let root = JsonPath::root();
        assert!(root.is_root());
        assert!(root.is_empty());
        assert_eq!(root.len(), 0);
    }

    #[test]
    fn test_path_key_builder() {
        let path = JsonPath::root().key("user").key("name");
        assert_eq!(path.len(), 2);
        assert!(!path.is_root());
        assert_eq!(
            path.segments(),
            &[
                PathSegment::Key("user".to_string()),
                PathSegment::Key("name".to_string())
            ]
        );
    }

    #[test]
    fn test_path_index_builder() {
        let path = JsonPath::root().key("items").index(0);
        assert_eq!(path.len(), 2);
        assert_eq!(
            path.segments(),
            &[PathSegment::Key("items".to_string()), PathSegment::Index(0)]
        );
    }

    #[test]
    fn test_path_push_methods() {
        let mut path = JsonPath::root();
        path.push_key("user");
        path.push_index(0);
        path.push_key("name");
        assert_eq!(path.len(), 3);
    }

    #[test]
    fn test_path_parse_simple_key() {
        let path: JsonPath = "user".parse().unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path.segments(), &[PathSegment::Key("user".to_string())]);
    }

    #[test]
    fn test_path_parse_dotted_keys() {
        let path: JsonPath = "user.name".parse().unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(
            path.segments(),
            &[
                PathSegment::Key("user".to_string()),
                PathSegment::Key("name".to_string())
            ]
        );
    }

    #[test]
    fn test_path_parse_leading_dot() {
        let path: JsonPath = ".user.name".parse().unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(
            path.segments(),
            &[
                PathSegment::Key("user".to_string()),
                PathSegment::Key("name".to_string())
            ]
        );
    }

    #[test]
    fn test_path_parse_array_index() {
        let path: JsonPath = "[0]".parse().unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path.segments(), &[PathSegment::Index(0)]);
    }

    #[test]
    fn test_path_parse_key_then_index() {
        let path: JsonPath = "items[0]".parse().unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(
            path.segments(),
            &[PathSegment::Key("items".to_string()), PathSegment::Index(0)]
        );
    }

    #[test]
    fn test_path_parse_complex() {
        let path: JsonPath = "users[0].profile.settings[2].value".parse().unwrap();
        assert_eq!(path.len(), 6);
        assert_eq!(
            path.segments(),
            &[
                PathSegment::Key("users".to_string()),
                PathSegment::Index(0),
                PathSegment::Key("profile".to_string()),
                PathSegment::Key("settings".to_string()),
                PathSegment::Index(2),
                PathSegment::Key("value".to_string()),
            ]
        );
    }

    #[test]
    fn test_path_parse_empty() {
        let path: JsonPath = "".parse().unwrap();
        assert!(path.is_root());
    }

    #[test]
    fn test_path_parse_with_underscore() {
        let path: JsonPath = "user_name".parse().unwrap();
        assert_eq!(
            path.segments(),
            &[PathSegment::Key("user_name".to_string())]
        );
    }

    #[test]
    fn test_path_parse_with_hyphen() {
        let path: JsonPath = "content-type".parse().unwrap();
        assert_eq!(
            path.segments(),
            &[PathSegment::Key("content-type".to_string())]
        );
    }

    #[test]
    fn test_path_parse_error_unclosed_bracket() {
        let result: Result<JsonPath, _> = "items[0".parse();
        assert!(matches!(result, Err(PathParseError::UnclosedBracket(_))));
    }

    #[test]
    fn test_path_parse_error_invalid_index() {
        let result: Result<JsonPath, _> = "items[abc]".parse();
        assert!(matches!(result, Err(PathParseError::InvalidIndex(_, _))));
    }

    #[test]
    fn test_path_parse_error_empty_key() {
        let result: Result<JsonPath, _> = "user.".parse();
        assert!(matches!(result, Err(PathParseError::EmptyKey(_))));
    }

    #[test]
    fn test_path_parent() {
        let path = JsonPath::root().key("user").key("name");
        let parent = path.parent().unwrap();
        assert_eq!(parent.len(), 1);
        assert_eq!(parent.segments(), &[PathSegment::Key("user".to_string())]);

        let grandparent = parent.parent().unwrap();
        assert!(grandparent.is_root());

        assert!(grandparent.parent().is_none());
    }

    #[test]
    fn test_path_last_segment() {
        let path = JsonPath::root().key("user").index(0);
        assert_eq!(path.last_segment(), Some(&PathSegment::Index(0)));

        let root = JsonPath::root();
        assert_eq!(root.last_segment(), None);
    }

    #[test]
    fn test_path_is_ancestor_of() {
        let root = JsonPath::root();
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");
        let items = JsonPath::root().key("items");

        // Root is ancestor of all
        assert!(root.is_ancestor_of(&root));
        assert!(root.is_ancestor_of(&user));
        assert!(root.is_ancestor_of(&user_name));

        // Paths are ancestors of themselves
        assert!(user.is_ancestor_of(&user));
        assert!(user_name.is_ancestor_of(&user_name));

        // Parent is ancestor of child
        assert!(user.is_ancestor_of(&user_name));

        // Child is not ancestor of parent
        assert!(!user_name.is_ancestor_of(&user));

        // Unrelated paths are not ancestors
        assert!(!user.is_ancestor_of(&items));
        assert!(!items.is_ancestor_of(&user));
    }

    #[test]
    fn test_path_is_descendant_of() {
        let root = JsonPath::root();
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");

        // All paths are descendants of root
        assert!(root.is_descendant_of(&root));
        assert!(user.is_descendant_of(&root));
        assert!(user_name.is_descendant_of(&root));

        // Paths are descendants of themselves
        assert!(user.is_descendant_of(&user));

        // Child is descendant of parent
        assert!(user_name.is_descendant_of(&user));

        // Parent is not descendant of child
        assert!(!user.is_descendant_of(&user_name));
    }

    #[test]
    fn test_path_overlaps() {
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");
        let items = JsonPath::root().key("items");

        // Ancestor/descendant paths overlap
        assert!(user.overlaps(&user_name));
        assert!(user_name.overlaps(&user));

        // Paths overlap with themselves
        assert!(user.overlaps(&user));

        // Unrelated paths don't overlap
        assert!(!user.overlaps(&items));
        assert!(!items.overlaps(&user_name));
    }

    #[test]
    fn test_strict_ancestor_descendant() {
        let root = JsonPath::root();
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");
        let items = JsonPath::root().key("items");

        // Root is strict ancestor of non-root paths
        assert!(root.is_strict_ancestor_of(&user));
        assert!(root.is_strict_ancestor_of(&user_name));

        // Path is not a strict ancestor of itself
        assert!(!user.is_strict_ancestor_of(&user));
        assert!(!root.is_strict_ancestor_of(&root));

        // Parent is strict ancestor of child
        assert!(user.is_strict_ancestor_of(&user_name));
        assert!(!user_name.is_strict_ancestor_of(&user));

        // Unrelated paths are not strict ancestors
        assert!(!user.is_strict_ancestor_of(&items));
        assert!(!items.is_strict_ancestor_of(&user));

        // Strict descendant is inverse
        assert!(user.is_strict_descendant_of(&root));
        assert!(user_name.is_strict_descendant_of(&user));
        assert!(user_name.is_strict_descendant_of(&root));
        assert!(!user.is_strict_descendant_of(&user));
    }

    #[test]
    fn test_common_ancestor() {
        let root = JsonPath::root();
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");
        let user_email = JsonPath::root().key("user").key("email");
        let items = JsonPath::root().key("items");

        // Common ancestor of path with itself is the path
        assert_eq!(user.common_ancestor(&user), user);

        // Common ancestor of parent/child is parent
        assert_eq!(user.common_ancestor(&user_name), user);
        assert_eq!(user_name.common_ancestor(&user), user);

        // Common ancestor of siblings is their parent
        assert_eq!(user_name.common_ancestor(&user_email), user);

        // Common ancestor of unrelated paths is root
        assert_eq!(user.common_ancestor(&items), root);

        // Common ancestor with root is root
        assert_eq!(user.common_ancestor(&root), root);
    }

    #[test]
    fn test_is_affected_by() {
        let user = JsonPath::root().key("user");
        let user_name = JsonPath::root().key("user").key("name");
        let items = JsonPath::root().key("items");

        // Write to parent affects child
        assert!(user_name.is_affected_by(&user));

        // Write to child affects parent (partial modification)
        assert!(user.is_affected_by(&user_name));

        // Write to self affects self
        assert!(user.is_affected_by(&user));

        // Unrelated paths don't affect each other
        assert!(!user.is_affected_by(&items));
        assert!(!items.is_affected_by(&user_name));
    }

    #[test]
    fn test_path_to_string() {
        assert_eq!(JsonPath::root().to_path_string(), "");
        assert_eq!(JsonPath::root().key("user").to_path_string(), "user");
        assert_eq!(
            JsonPath::root().key("user").key("name").to_path_string(),
            "user.name"
        );
        assert_eq!(
            JsonPath::root().key("items").index(0).to_path_string(),
            "items[0]"
        );
        assert_eq!(
            JsonPath::root()
                .key("items")
                .index(0)
                .key("name")
                .to_path_string(),
            "items[0].name"
        );
    }

    #[test]
    fn test_path_display() {
        let path = JsonPath::root().key("user").key("name");
        assert_eq!(format!("{}", path), "user.name");
    }

    #[test]
    fn test_path_default() {
        let path = JsonPath::default();
        assert!(path.is_root());
    }

    #[test]
    fn test_path_clone() {
        let path1 = JsonPath::root().key("user");
        let path2 = path1.clone();
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_path_equality() {
        let path1 = JsonPath::root().key("user").key("name");
        let path2: JsonPath = "user.name".parse().unwrap();
        let path3 = JsonPath::root().key("user").key("email");

        assert_eq!(path1, path2);
        assert_ne!(path1, path3);
    }

    #[test]
    fn test_path_hash() {
        use std::collections::HashSet;

        let path1 = JsonPath::root().key("user");
        let path2: JsonPath = "user".parse().unwrap();
        let path3 = JsonPath::root().key("items");

        let mut set = HashSet::new();
        set.insert(path1.clone());
        set.insert(path2); // Same as path1
        set.insert(path3);

        assert_eq!(set.len(), 2);
        assert!(set.contains(&path1));
    }

    #[test]
    fn test_path_serialization() {
        let path = JsonPath::root().key("user").index(0).key("name");
        let json = serde_json::to_string(&path).unwrap();
        let path2: JsonPath = serde_json::from_str(&json).unwrap();
        assert_eq!(path, path2);
    }

    #[test]
    fn test_path_segment_display() {
        assert_eq!(format!("{}", PathSegment::Key("foo".to_string())), ".foo");
        assert_eq!(format!("{}", PathSegment::Index(42)), "[42]");
    }

    #[test]
    fn test_path_from_segments() {
        let segments = vec![PathSegment::Key("user".to_string()), PathSegment::Index(0)];
        let path = JsonPath::from_segments(segments.clone());
        assert_eq!(path.segments(), &segments);
    }

    // ========================================
    // JsonPatch Tests
    // ========================================

    #[test]
    fn test_patch_set() {
        let patch = JsonPatch::set("user.name", JsonValue::from("Alice"));
        assert!(patch.is_set());
        assert!(!patch.is_delete());
        assert_eq!(patch.path().to_path_string(), "user.name");
        assert_eq!(patch.value().unwrap().as_str(), Some("Alice"));
    }

    #[test]
    fn test_patch_set_at() {
        let path = JsonPath::root().key("user").key("name");
        let patch = JsonPatch::set_at(path.clone(), JsonValue::from("Bob"));
        assert!(patch.is_set());
        assert_eq!(patch.path(), &path);
    }

    #[test]
    fn test_patch_delete() {
        let patch = JsonPatch::delete("user.email");
        assert!(!patch.is_set());
        assert!(patch.is_delete());
        assert_eq!(patch.path().to_path_string(), "user.email");
        assert!(patch.value().is_none());
    }

    #[test]
    fn test_patch_delete_at() {
        let path = JsonPath::root().key("user").key("email");
        let patch = JsonPatch::delete_at(path.clone());
        assert!(patch.is_delete());
        assert_eq!(patch.path(), &path);
    }

    #[test]
    fn test_patch_conflicts_with_overlapping() {
        let patch1 = JsonPatch::set("user", JsonValue::object());
        let patch2 = JsonPatch::set("user.name", JsonValue::from("Alice"));

        // Parent/child paths conflict
        assert!(patch1.conflicts_with(&patch2));
        assert!(patch2.conflicts_with(&patch1));
    }

    #[test]
    fn test_patch_conflicts_with_same_path() {
        let patch1 = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let patch2 = JsonPatch::set("user.name", JsonValue::from("Bob"));

        // Same path conflicts
        assert!(patch1.conflicts_with(&patch2));
    }

    #[test]
    fn test_patch_no_conflict_different_paths() {
        let patch1 = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let patch2 = JsonPatch::set("user.email", JsonValue::from("alice@example.com"));

        // Sibling paths don't conflict
        assert!(!patch1.conflicts_with(&patch2));
        assert!(!patch2.conflicts_with(&patch1));
    }

    #[test]
    fn test_patch_delete_conflicts() {
        let set_patch = JsonPatch::set("user.profile", JsonValue::object());
        let delete_patch = JsonPatch::delete("user");

        // Delete of ancestor conflicts with set of descendant
        assert!(set_patch.conflicts_with(&delete_patch));
        assert!(delete_patch.conflicts_with(&set_patch));
    }

    #[test]
    fn test_patch_display_set() {
        let patch = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let s = format!("{}", patch);
        assert!(s.starts_with("SET"));
        assert!(s.contains("user.name"));
        assert!(s.contains("Alice"));
    }

    #[test]
    fn test_patch_display_delete() {
        let patch = JsonPatch::delete("user.email");
        let s = format!("{}", patch);
        assert!(s.starts_with("DELETE"));
        assert!(s.contains("user.email"));
    }

    #[test]
    fn test_patch_clone() {
        let patch1 = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let patch2 = patch1.clone();
        assert_eq!(patch1, patch2);
    }

    #[test]
    fn test_patch_equality() {
        let patch1 = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let patch2 = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let patch3 = JsonPatch::set("user.name", JsonValue::from("Bob"));
        let patch4 = JsonPatch::delete("user.name");

        assert_eq!(patch1, patch2);
        assert_ne!(patch1, patch3);
        assert_ne!(patch1, patch4);
    }

    #[test]
    fn test_patch_serialization() {
        let patch = JsonPatch::set("user.name", JsonValue::from("Alice"));
        let json = serde_json::to_string(&patch).unwrap();
        let patch2: JsonPatch = serde_json::from_str(&json).unwrap();
        assert_eq!(patch, patch2);
    }

    #[test]
    fn test_patch_delete_serialization() {
        let patch = JsonPatch::delete("user.email");
        let json = serde_json::to_string(&patch).unwrap();
        let patch2: JsonPatch = serde_json::from_str(&json).unwrap();
        assert_eq!(patch, patch2);
    }

    #[test]
    fn test_patch_root_conflicts_with_all() {
        let root_patch = JsonPatch::set("", JsonValue::object());
        let other_patch = JsonPatch::set("user.name", JsonValue::from("Alice"));

        // Root path conflicts with all paths
        assert!(root_patch.conflicts_with(&other_patch));
        assert!(other_patch.conflicts_with(&root_patch));
    }

    #[test]
    fn test_patch_with_array_index() {
        let patch1 = JsonPatch::set("items[0].name", JsonValue::from("First"));
        let patch2 = JsonPatch::set("items[1].name", JsonValue::from("Second"));

        // Different array indices don't conflict
        assert!(!patch1.conflicts_with(&patch2));

        let patch3 = JsonPatch::set("items", JsonValue::array());
        // Parent of array conflicts with child path
        assert!(patch3.conflicts_with(&patch1));
    }

    // ========================================
    // Document Size Limits Tests
    // ========================================

    #[test]
    fn test_limit_constants() {
        // Verify limit constants are defined with expected values
        assert_eq!(MAX_DOCUMENT_SIZE, 16 * 1024 * 1024); // 16 MB
        assert_eq!(MAX_NESTING_DEPTH, 100);
        assert_eq!(MAX_PATH_LENGTH, 256);
        assert_eq!(MAX_ARRAY_SIZE, 1_000_000);
    }

    #[test]
    fn test_nesting_depth_primitive() {
        assert_eq!(JsonValue::null().nesting_depth(), 0);
        assert_eq!(JsonValue::from(true).nesting_depth(), 0);
        assert_eq!(JsonValue::from(42i64).nesting_depth(), 0);
        assert_eq!(JsonValue::from("hello").nesting_depth(), 0);
    }

    #[test]
    fn test_nesting_depth_simple_object() {
        let v = JsonValue::object();
        assert_eq!(v.nesting_depth(), 1);
    }

    #[test]
    fn test_nesting_depth_simple_array() {
        let v = JsonValue::array();
        assert_eq!(v.nesting_depth(), 1);
    }

    #[test]
    fn test_nesting_depth_nested() {
        let v: JsonValue = r#"{"a": {"b": {"c": 1}}}"#.parse().unwrap();
        assert_eq!(v.nesting_depth(), 3);
    }

    #[test]
    fn test_nesting_depth_mixed() {
        let v: JsonValue = r#"{"arr": [{"nested": [1, 2]}]}"#.parse().unwrap();
        assert_eq!(v.nesting_depth(), 4);
    }

    #[test]
    fn test_max_array_size_empty() {
        let v = JsonValue::object();
        assert_eq!(v.max_array_size(), 0);
    }

    #[test]
    fn test_max_array_size_simple() {
        let v: JsonValue = r#"[1, 2, 3, 4, 5]"#.parse().unwrap();
        assert_eq!(v.max_array_size(), 5);
    }

    #[test]
    fn test_max_array_size_nested() {
        let v: JsonValue = r#"{"a": [1, 2], "b": [1, 2, 3, 4, 5, 6, 7]}"#.parse().unwrap();
        assert_eq!(v.max_array_size(), 7);
    }

    #[test]
    fn test_validate_size_ok() {
        let v = JsonValue::from("small document");
        assert!(v.validate_size().is_ok());
    }

    #[test]
    fn test_validate_depth_ok() {
        let v: JsonValue = r#"{"a": {"b": {"c": 1}}}"#.parse().unwrap();
        assert!(v.validate_depth().is_ok());
    }

    #[test]
    fn test_validate_array_size_ok() {
        let v: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        assert!(v.validate_array_size().is_ok());
    }

    #[test]
    fn test_validate_all_ok() {
        let v: JsonValue = r#"{"user": {"name": "Alice", "tags": [1, 2, 3]}}"#.parse().unwrap();
        assert!(v.validate().is_ok());
    }

    // ========================================
    // Limit Boundary Tests
    // ========================================

    #[test]
    fn test_nesting_at_max_depth_passes() {
        // Create a document with exactly MAX_NESTING_DEPTH levels
        // Each level is an object containing the next level
        let mut json_str = String::new();
        for _ in 0..MAX_NESTING_DEPTH {
            json_str.push_str(r#"{"a":"#);
        }
        json_str.push('1');
        for _ in 0..MAX_NESTING_DEPTH {
            json_str.push('}');
        }

        let v: JsonValue = json_str.parse().expect("Should parse valid JSON");
        assert_eq!(
            v.nesting_depth(),
            MAX_NESTING_DEPTH,
            "Should have exactly MAX_NESTING_DEPTH levels"
        );
        assert!(
            v.validate_depth().is_ok(),
            "Document at exactly MAX_NESTING_DEPTH should pass validation"
        );
    }

    #[test]
    fn test_nesting_exceeds_max_depth_fails() {
        // Create a document with MAX_NESTING_DEPTH + 1 levels
        let depth = MAX_NESTING_DEPTH + 1;
        let mut json_str = String::new();
        for _ in 0..depth {
            json_str.push_str(r#"{"a":"#);
        }
        json_str.push('1');
        for _ in 0..depth {
            json_str.push('}');
        }

        let v: JsonValue = json_str.parse().expect("Should parse valid JSON");
        assert_eq!(
            v.nesting_depth(),
            depth,
            "Should have exactly MAX_NESTING_DEPTH + 1 levels"
        );

        let result = v.validate_depth();
        assert!(
            result.is_err(),
            "Should fail validation when exceeding MAX_NESTING_DEPTH"
        );

        match result {
            Err(JsonLimitError::NestingTooDeep { depth: d, max }) => {
                assert_eq!(d, depth);
                assert_eq!(max, MAX_NESTING_DEPTH);
            }
            _ => panic!("Expected NestingTooDeep error"),
        }
    }

    #[test]
    fn test_nesting_with_arrays_at_boundary() {
        // Arrays also count as nesting. Test alternating objects and arrays.
        // Each pair adds 2 levels of nesting
        let pairs = MAX_NESTING_DEPTH / 2;
        let mut json_str = String::new();
        for _ in 0..pairs {
            json_str.push_str(r#"{"a":["#);
        }
        json_str.push('1');
        for _ in 0..pairs {
            json_str.push_str("]}");
        }

        let v: JsonValue = json_str.parse().expect("Should parse valid JSON");
        let depth = v.nesting_depth();
        assert_eq!(depth, pairs * 2, "Each object-array pair adds 2 levels");
        assert!(
            v.validate_depth().is_ok(),
            "Nesting within limit should pass"
        );
    }

    #[test]
    fn test_array_size_validation_logic() {
        // We can't easily test MAX_ARRAY_SIZE (1M elements) due to memory,
        // but we can verify the validation logic works correctly.
        // Create an array and verify max_array_size() reports correctly.
        let v: JsonValue = r#"[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"#.parse().unwrap();
        assert_eq!(v.max_array_size(), 10);
        assert!(v.validate_array_size().is_ok());

        // Verify nested arrays report the maximum
        let v: JsonValue = r#"{"outer": [1, 2], "inner": [[1, 2, 3, 4, 5]]}"#.parse().unwrap();
        assert_eq!(v.max_array_size(), 5, "Should report max of all arrays");
    }

    #[test]
    fn test_document_size_validation_logic() {
        // We can't easily test MAX_DOCUMENT_SIZE (16MB) due to memory,
        // but we can verify the size calculation works.
        let v = JsonValue::from("test");
        let size = v.size_bytes();
        assert!(size > 0, "Should report non-zero size");
        assert!(
            v.validate_size().is_ok(),
            "Small document should pass size validation"
        );

        // Larger string should have larger size
        let large_str = "x".repeat(10000);
        let v_large = JsonValue::from(large_str.as_str());
        assert!(
            v_large.size_bytes() > v.size_bytes(),
            "Larger content should have larger size"
        );
    }

    #[test]
    fn test_validate_catches_first_limit_exceeded() {
        // Create a document that exceeds nesting depth
        let depth = MAX_NESTING_DEPTH + 5;
        let mut json_str = String::new();
        for _ in 0..depth {
            json_str.push_str(r#"{"a":"#);
        }
        json_str.push('1');
        for _ in 0..depth {
            json_str.push('}');
        }

        let v: JsonValue = json_str.parse().expect("Should parse valid JSON");

        // validate() should catch the nesting limit
        let result = v.validate();
        assert!(result.is_err(), "validate() should fail");
        assert!(
            matches!(result, Err(JsonLimitError::NestingTooDeep { .. })),
            "Should be NestingTooDeep error"
        );
    }

    #[test]
    fn test_path_validate_ok() {
        let path = JsonPath::root().key("user").key("name");
        assert!(path.validate().is_ok());
    }

    #[test]
    fn test_path_validate_long_path() {
        let mut path = JsonPath::root();
        for i in 0..300 {
            path.push_key(format!("key{}", i));
        }
        let result = path.validate();
        assert!(matches!(result, Err(JsonLimitError::PathTooLong { .. })));
    }

    #[test]
    fn test_limit_error_display() {
        let err = JsonLimitError::DocumentTooLarge {
            size: 20_000_000,
            max: MAX_DOCUMENT_SIZE,
        };
        let s = format!("{}", err);
        assert!(s.contains("20000000"));
        assert!(s.contains("16777216"));
    }

    #[test]
    fn test_limit_error_nesting_display() {
        let err = JsonLimitError::NestingTooDeep {
            depth: 150,
            max: MAX_NESTING_DEPTH,
        };
        let s = format!("{}", err);
        assert!(s.contains("150"));
        assert!(s.contains("100"));
    }

    #[test]
    fn test_limit_error_path_display() {
        let err = JsonLimitError::PathTooLong {
            length: 300,
            max: MAX_PATH_LENGTH,
        };
        let s = format!("{}", err);
        assert!(s.contains("300"));
        assert!(s.contains("256"));
    }

    #[test]
    fn test_limit_error_array_display() {
        let err = JsonLimitError::ArrayTooLarge {
            size: 2_000_000,
            max: MAX_ARRAY_SIZE,
        };
        let s = format!("{}", err);
        assert!(s.contains("2000000"));
        assert!(s.contains("1000000"));
    }

    #[test]
    fn test_limit_error_equality() {
        let err1 = JsonLimitError::DocumentTooLarge { size: 100, max: 50 };
        let err2 = JsonLimitError::DocumentTooLarge { size: 100, max: 50 };
        let err3 = JsonLimitError::DocumentTooLarge { size: 200, max: 50 };
        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_limit_error_clone() {
        let err = JsonLimitError::PathTooLong {
            length: 300,
            max: 256,
        };
        let err2 = err.clone();
        assert_eq!(err, err2);
    }

    // =========================================================================
    // Path Operations Tests
    // =========================================================================

    #[test]
    fn test_get_at_path_root() {
        let value = JsonValue::from(42i64);
        let result = get_at_path(&value, &JsonPath::root());
        assert_eq!(result, Some(&value));
    }

    #[test]
    fn test_get_at_path_simple_object() {
        let json: JsonValue = r#"{"foo": 42}"#.parse().unwrap();
        let path: JsonPath = "foo".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_i64(), Some(42));
    }

    #[test]
    fn test_get_at_path_nested_object() {
        let json: JsonValue = r#"{"foo": {"bar": 42}}"#.parse().unwrap();
        let path: JsonPath = "foo.bar".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_i64(), Some(42));
    }

    #[test]
    fn test_get_at_path_array_index() {
        let json: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        let path: JsonPath = "[1]".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_get_at_path_mixed() {
        let json: JsonValue = r#"{"items": [{"name": "Alice"}, {"name": "Bob"}]}"#
            .parse()
            .unwrap();
        let path: JsonPath = "items[1].name".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_str(), Some("Bob"));
    }

    #[test]
    fn test_get_at_path_missing_key() {
        let json: JsonValue = r#"{"foo": 42}"#.parse().unwrap();
        let path: JsonPath = "bar".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_at_path_missing_index() {
        let json: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        let path: JsonPath = "[10]".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_at_path_type_mismatch_object_as_array() {
        let json: JsonValue = r#"{"foo": 42}"#.parse().unwrap();
        let path: JsonPath = "[0]".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_at_path_type_mismatch_array_as_object() {
        let json: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        let path: JsonPath = "foo".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_at_path_type_mismatch_scalar() {
        let json: JsonValue = JsonValue::from(42i64);
        let path: JsonPath = "foo".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_at_path_deeply_nested() {
        let json: JsonValue = r#"{"a": {"b": {"c": {"d": {"e": 42}}}}}"#.parse().unwrap();
        let path: JsonPath = "a.b.c.d.e".parse().unwrap();
        let result = get_at_path(&json, &path);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_i64(), Some(42));
    }

    #[test]
    fn test_get_at_path_mut_root() {
        let mut value = JsonValue::from(42i64);
        let result = get_at_path_mut(&mut value, &JsonPath::root());
        assert!(result.is_some());
        *result.unwrap() = JsonValue::from(100i64);
        assert_eq!(value.as_i64(), Some(100));
    }

    #[test]
    fn test_get_at_path_mut_modify_nested() {
        let mut json: JsonValue = r#"{"user": {"name": "Alice"}}"#.parse().unwrap();
        let path: JsonPath = "user.name".parse().unwrap();

        if let Some(name) = get_at_path_mut(&mut json, &path) {
            *name = JsonValue::from("Bob");
        }

        let result = get_at_path(&json, &path);
        assert_eq!(result.unwrap().as_str(), Some("Bob"));
    }

    #[test]
    fn test_get_at_path_mut_modify_array_element() {
        let mut json: JsonValue = r#"[10, 20, 30]"#.parse().unwrap();
        let path: JsonPath = "[1]".parse().unwrap();

        if let Some(elem) = get_at_path_mut(&mut json, &path) {
            *elem = JsonValue::from(999i64);
        }

        let result = get_at_path(&json, &path);
        assert_eq!(result.unwrap().as_i64(), Some(999));
    }

    #[test]
    fn test_get_at_path_mut_missing() {
        let mut json: JsonValue = r#"{"foo": 42}"#.parse().unwrap();
        let path: JsonPath = "bar".parse().unwrap();
        let result = get_at_path_mut(&mut json, &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_error_display() {
        let err = JsonPathError::TypeMismatch {
            expected: "object",
            found: "array",
        };
        let s = format!("{}", err);
        assert!(s.contains("object"));
        assert!(s.contains("array"));

        let err2 = JsonPathError::IndexOutOfBounds { index: 10, len: 5 };
        let s2 = format!("{}", err2);
        assert!(s2.contains("10"));
        assert!(s2.contains("5"));

        let err3 = JsonPathError::NotFound;
        let s3 = format!("{}", err3);
        assert!(s3.contains("not found"));
    }

    #[test]
    fn test_json_path_error_equality() {
        let err1 = JsonPathError::IndexOutOfBounds { index: 5, len: 3 };
        let err2 = JsonPathError::IndexOutOfBounds { index: 5, len: 3 };
        let err3 = JsonPathError::IndexOutOfBounds { index: 10, len: 3 };
        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    // =========================================================================
    // Set at Path Tests
    // =========================================================================

    #[test]
    fn test_set_at_path_root() {
        let mut value = JsonValue::from(42i64);
        set_at_path(&mut value, &JsonPath::root(), JsonValue::from(100i64)).unwrap();
        assert_eq!(value.as_i64(), Some(100));
    }

    #[test]
    fn test_set_at_path_simple_object() {
        let mut json = JsonValue::object();
        let path: JsonPath = "name".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from("Alice")).unwrap();

        let result = get_at_path(&json, &path);
        assert_eq!(result.unwrap().as_str(), Some("Alice"));
    }

    #[test]
    fn test_set_at_path_nested_creates_intermediate() {
        let mut json = JsonValue::object();
        let path: JsonPath = "user.profile.name".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from("Bob")).unwrap();

        // Verify intermediate objects were created
        assert!(get_at_path(&json, &"user".parse().unwrap()).is_some());
        assert!(get_at_path(&json, &"user.profile".parse().unwrap()).is_some());
        assert_eq!(get_at_path(&json, &path).unwrap().as_str(), Some("Bob"));
    }

    #[test]
    fn test_set_at_path_creates_array_intermediate() {
        let mut json = JsonValue::object();
        let path: JsonPath = "items[0]".parse().unwrap();

        // First set up the array
        set_at_path(&mut json, &"items".parse().unwrap(), JsonValue::array()).unwrap();

        // Now set at index 0 (append)
        set_at_path(&mut json, &path, JsonValue::from(42i64)).unwrap();

        let result = get_at_path(&json, &path);
        assert_eq!(result.unwrap().as_i64(), Some(42));
    }

    #[test]
    fn test_set_at_path_overwrite_existing() {
        let mut json: JsonValue = r#"{"name": "Alice"}"#.parse().unwrap();
        let path: JsonPath = "name".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from("Bob")).unwrap();

        assert_eq!(get_at_path(&json, &path).unwrap().as_str(), Some("Bob"));
    }

    #[test]
    fn test_set_at_path_array_replace() {
        let mut json: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        let path: JsonPath = "[1]".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from(999i64)).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[1].as_i64(), Some(999));
    }

    #[test]
    fn test_set_at_path_array_append() {
        let mut json: JsonValue = r#"[1, 2]"#.parse().unwrap();
        let path: JsonPath = "[2]".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from(3i64)).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[2].as_i64(), Some(3));
    }

    #[test]
    fn test_set_at_path_array_out_of_bounds() {
        let mut json: JsonValue = r#"[1, 2]"#.parse().unwrap();
        let path: JsonPath = "[10]".parse().unwrap();
        let result = set_at_path(&mut json, &path, JsonValue::from(42i64));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonPathError::IndexOutOfBounds { .. }
        ));
    }

    #[test]
    fn test_set_at_path_type_mismatch_not_object() {
        let mut json = JsonValue::from(42i64);
        let path: JsonPath = "name".parse().unwrap();
        let result = set_at_path(&mut json, &path, JsonValue::from("value"));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonPathError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_set_at_path_type_mismatch_not_array() {
        let mut json = JsonValue::from(42i64);
        let path: JsonPath = "[0]".parse().unwrap();
        let result = set_at_path(&mut json, &path, JsonValue::from("value"));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonPathError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_set_at_path_deeply_nested() {
        let mut json = JsonValue::object();
        let path: JsonPath = "a.b.c.d.e".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from(42i64)).unwrap();

        assert_eq!(get_at_path(&json, &path).unwrap().as_i64(), Some(42));
    }

    #[test]
    fn test_set_at_path_mixed_path() {
        let mut json = JsonValue::object();

        // First create the structure
        set_at_path(&mut json, &"items".parse().unwrap(), JsonValue::array()).unwrap();
        set_at_path(&mut json, &"items[0]".parse().unwrap(), JsonValue::object()).unwrap();

        // Now set nested value
        let path: JsonPath = "items[0].name".parse().unwrap();
        set_at_path(&mut json, &path, JsonValue::from("Item 1")).unwrap();

        assert_eq!(get_at_path(&json, &path).unwrap().as_str(), Some("Item 1"));
    }

    // =========================================================================
    // Delete at Path Tests
    // =========================================================================

    #[test]
    fn test_delete_at_path_root() {
        let mut json = JsonValue::from(42i64);
        let deleted = delete_at_path(&mut json, &JsonPath::root()).unwrap();
        assert_eq!(deleted.unwrap().as_i64(), Some(42));
        assert!(json.is_null());
    }

    #[test]
    fn test_delete_at_path_object_key() {
        let mut json: JsonValue = r#"{"name": "Alice", "age": 30}"#.parse().unwrap();
        let path: JsonPath = "name".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert_eq!(deleted.unwrap().as_str(), Some("Alice"));
        assert!(get_at_path(&json, &path).is_none());
        // age should still exist
        assert!(get_at_path(&json, &"age".parse().unwrap()).is_some());
    }

    #[test]
    fn test_delete_at_path_nested_object() {
        let mut json: JsonValue = r#"{"user": {"name": "Bob", "email": "bob@example.com"}}"#
            .parse()
            .unwrap();
        let path: JsonPath = "user.name".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert_eq!(deleted.unwrap().as_str(), Some("Bob"));
        assert!(get_at_path(&json, &path).is_none());
        // user.email should still exist
        assert!(get_at_path(&json, &"user.email".parse().unwrap()).is_some());
    }

    #[test]
    fn test_delete_at_path_array_element() {
        let mut json: JsonValue = r#"[1, 2, 3]"#.parse().unwrap();
        let path: JsonPath = "[1]".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert_eq!(deleted.unwrap().as_i64(), Some(2));
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // Verify indices shifted
        assert_eq!(arr[0].as_i64(), Some(1));
        assert_eq!(arr[1].as_i64(), Some(3)); // Was at index 2
    }

    #[test]
    fn test_delete_at_path_array_first() {
        let mut json: JsonValue = r#"["a", "b", "c"]"#.parse().unwrap();
        let path: JsonPath = "[0]".parse().unwrap();
        delete_at_path(&mut json, &path).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].as_str(), Some("b"));
        assert_eq!(arr[1].as_str(), Some("c"));
    }

    #[test]
    fn test_delete_at_path_array_last() {
        let mut json: JsonValue = r#"["a", "b", "c"]"#.parse().unwrap();
        let path: JsonPath = "[2]".parse().unwrap();
        delete_at_path(&mut json, &path).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].as_str(), Some("a"));
        assert_eq!(arr[1].as_str(), Some("b"));
    }

    #[test]
    fn test_delete_at_path_missing_key() {
        let mut json: JsonValue = r#"{"name": "Alice"}"#.parse().unwrap();
        let path: JsonPath = "missing".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert!(deleted.is_none());
    }

    #[test]
    fn test_delete_at_path_missing_index() {
        let mut json: JsonValue = r#"[1, 2]"#.parse().unwrap();
        let path: JsonPath = "[10]".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert!(deleted.is_none());
    }

    #[test]
    fn test_delete_at_path_missing_parent() {
        let mut json: JsonValue = r#"{"foo": 1}"#.parse().unwrap();
        let path: JsonPath = "bar.baz".parse().unwrap();
        let result = delete_at_path(&mut json, &path);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), JsonPathError::NotFound));
    }

    #[test]
    fn test_delete_at_path_type_mismatch() {
        let mut json = JsonValue::from(42i64);
        let path: JsonPath = "name".parse().unwrap();
        let result = delete_at_path(&mut json, &path);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonPathError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_delete_at_path_nested_array() {
        let mut json: JsonValue =
            r#"{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}"#.parse().unwrap();
        let path: JsonPath = "items[1]".parse().unwrap();
        let deleted = delete_at_path(&mut json, &path).unwrap();

        assert!(deleted.is_some());
        let arr = get_at_path(&json, &"items".parse().unwrap())
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(arr.len(), 2);
    }

    // =========================================================================
    // Apply Patches Tests
    // =========================================================================

    #[test]
    fn test_apply_patches_empty() {
        let mut json = JsonValue::object();
        let patches: Vec<JsonPatch> = vec![];
        apply_patches(&mut json, &patches).unwrap();
        assert!(json.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_apply_patches_single_set() {
        let mut json = JsonValue::object();
        let patches = vec![JsonPatch::set("name", JsonValue::from("Alice"))];
        apply_patches(&mut json, &patches).unwrap();

        assert_eq!(
            get_at_path(&json, &"name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Alice")
        );
    }

    #[test]
    fn test_apply_patches_multiple_sets() {
        let mut json = JsonValue::object();
        let patches = vec![
            JsonPatch::set("name", JsonValue::from("Alice")),
            JsonPatch::set("age", JsonValue::from(30i64)),
            JsonPatch::set("active", JsonValue::from(true)),
        ];
        apply_patches(&mut json, &patches).unwrap();

        assert_eq!(
            get_at_path(&json, &"name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Alice")
        );
        assert_eq!(
            get_at_path(&json, &"age".parse().unwrap())
                .unwrap()
                .as_i64(),
            Some(30)
        );
        assert_eq!(
            get_at_path(&json, &"active".parse().unwrap())
                .unwrap()
                .as_bool(),
            Some(true)
        );
    }

    #[test]
    fn test_apply_patches_set_and_delete() {
        let mut json: JsonValue = r#"{"name": "Alice", "age": 30}"#.parse().unwrap();
        let patches = vec![
            JsonPatch::set("email", JsonValue::from("alice@example.com")),
            JsonPatch::delete("age"),
        ];
        apply_patches(&mut json, &patches).unwrap();

        assert!(get_at_path(&json, &"email".parse().unwrap()).is_some());
        assert!(get_at_path(&json, &"age".parse().unwrap()).is_none());
        assert!(get_at_path(&json, &"name".parse().unwrap()).is_some());
    }

    #[test]
    fn test_apply_patches_nested() {
        let mut json = JsonValue::object();
        let patches = vec![
            JsonPatch::set("user.profile.name", JsonValue::from("Bob")),
            JsonPatch::set("user.profile.email", JsonValue::from("bob@example.com")),
        ];
        apply_patches(&mut json, &patches).unwrap();

        assert_eq!(
            get_at_path(&json, &"user.profile.name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Bob")
        );
        assert_eq!(
            get_at_path(&json, &"user.profile.email".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("bob@example.com")
        );
    }

    #[test]
    fn test_apply_patches_overwrite() {
        let mut json: JsonValue = r#"{"name": "Alice"}"#.parse().unwrap();
        let patches = vec![
            JsonPatch::set("name", JsonValue::from("Bob")),
            JsonPatch::set("name", JsonValue::from("Charlie")),
        ];
        apply_patches(&mut json, &patches).unwrap();

        // Last write wins
        assert_eq!(
            get_at_path(&json, &"name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Charlie")
        );
    }

    #[test]
    fn test_apply_patches_set_then_delete() {
        let mut json = JsonValue::object();
        let patches = vec![
            JsonPatch::set("temp", JsonValue::from("value")),
            JsonPatch::delete("temp"),
        ];
        apply_patches(&mut json, &patches).unwrap();

        assert!(get_at_path(&json, &"temp".parse().unwrap()).is_none());
    }

    #[test]
    fn test_apply_patches_delete_then_set() {
        let mut json: JsonValue = r#"{"name": "Alice"}"#.parse().unwrap();
        let patches = vec![
            JsonPatch::delete("name"),
            JsonPatch::set("name", JsonValue::from("Bob")),
        ];
        apply_patches(&mut json, &patches).unwrap();

        assert_eq!(
            get_at_path(&json, &"name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Bob")
        );
    }

    #[test]
    fn test_apply_patches_error_propagation() {
        let mut json = JsonValue::from(42i64);
        let patches = vec![JsonPatch::set("name", JsonValue::from("Alice"))];
        let result = apply_patches(&mut json, &patches);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonPathError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_apply_patches_partial_failure() {
        let mut json = JsonValue::object();
        let patches = vec![
            JsonPatch::set("name", JsonValue::from("Alice")),
            JsonPatch::set("[0]", JsonValue::from("invalid")), // type mismatch - object not array
        ];
        let result = apply_patches(&mut json, &patches);

        // First patch should have been applied before failure
        assert!(result.is_err());
        assert_eq!(
            get_at_path(&json, &"name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Alice")
        );
    }

    #[test]
    fn test_apply_patches_with_arrays() {
        let mut json = JsonValue::object();
        let patches = vec![
            JsonPatch::set("items", JsonValue::array()),
            JsonPatch::set("items[0]", JsonValue::from("first")),
            JsonPatch::set("items[1]", JsonValue::from("second")),
        ];
        apply_patches(&mut json, &patches).unwrap();

        let items = get_at_path(&json, &"items".parse().unwrap())
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_str(), Some("first"));
        assert_eq!(items[1].as_str(), Some("second"));
    }
}
