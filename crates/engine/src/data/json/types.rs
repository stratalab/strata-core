//! JSON input types and path operations.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;

const MAX_DOCUMENT_ID_BYTES: usize = u16::MAX as usize;
const MAX_INDEX_NAME_BYTES: usize = 256;
const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_NESTING_DEPTH: usize = 100;
const MAX_ARRAY_SIZE: usize = 1_000_000;
const MAX_PATH_SEGMENTS: usize = 256;

/// JSON document identifier.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct JsonDocumentId(String);

impl JsonDocumentId {
    /// Creates a validated document id.
    pub fn new(id: impl Into<String>) -> Result<Self, EngineError> {
        let id = id.into();
        validate_component(
            &id,
            MAX_DOCUMENT_ID_BYTES,
            "invalid_argument.engine.json_document_id",
            "JSON document id",
        )?;
        Ok(Self(id))
    }

    #[must_use]
    /// Returns the id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for JsonDocumentId {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for JsonDocumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// JSON index name.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct JsonIndexName(String);

impl JsonIndexName {
    /// Creates a validated index name.
    pub fn new(name: impl Into<String>) -> Result<Self, EngineError> {
        let name = name.into();
        validate_component(
            &name,
            MAX_INDEX_NAME_BYTES,
            "invalid_argument.engine.json_index_name",
            "JSON index name",
        )?;
        if name.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_index_name_reserved",
                "JSON index name is reserved for engine internals",
            ));
        }
        Ok(Self(name))
    }

    #[must_use]
    /// Returns the index name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for JsonIndexName {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for JsonIndexName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// JSON value wrapper with engine-owned validation.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct JsonValue(Value);

impl JsonValue {
    /// Creates a JSON value after applying document limits.
    pub fn new(value: Value) -> Result<Self, EngineError> {
        let wrapped = Self(value);
        wrapped.validate()?;
        Ok(wrapped)
    }

    /// Creates a JSON null value.
    pub fn null() -> Self {
        Self(Value::Null)
    }

    /// Creates an empty JSON object.
    pub fn object() -> Self {
        Self(Value::Object(Map::new()))
    }

    #[must_use]
    /// Returns the wrapped value.
    pub fn as_inner(&self) -> &Value {
        &self.0
    }

    #[must_use]
    /// Consumes the wrapper and returns the JSON value.
    pub fn into_inner(self) -> Value {
        self.0
    }

    pub(crate) fn from_stored(value: Value) -> Self {
        Self(value)
    }

    pub(crate) fn validate(&self) -> Result<(), EngineError> {
        // Enforce the nesting bound FIRST, with an iterative traversal, so a
        // pathologically deep value is rejected before the recursive
        // serialization/measurement below can overflow the stack (finding U36).
        if exceeds_nesting_depth(&self.0, MAX_NESTING_DEPTH) {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_document_too_deep",
                "JSON document exceeds the maximum nesting depth",
            ));
        }
        let size = serde_json::to_vec(&self.0)
            .map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.json_value",
                    format!("JSON value cannot be serialized: {error}"),
                )
            })?
            .len();
        if size > MAX_DOCUMENT_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_document_too_large",
                "JSON document exceeds the maximum encoded size",
            ));
        }
        let array_size = max_array_size(&self.0);
        if array_size > MAX_ARRAY_SIZE {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_array_too_large",
                "JSON document contains an oversized array",
            ));
        }
        Ok(())
    }
}

impl TryFrom<Value> for JsonValue {
    type Error = EngineError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<JsonValue> for Value {
    fn from(value: JsonValue) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// One JSON path segment.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonPathSegment {
    /// Object field access.
    Key(String),
    /// Array element access.
    Index(usize),
}

/// Deterministic single-target JSON path.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
pub struct JsonPath(Vec<JsonPathSegment>);

impl JsonPath {
    /// Returns the root path.
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// Creates a path from validated segments.
    pub fn from_segments(segments: Vec<JsonPathSegment>) -> Result<Self, EngineError> {
        let path = Self(segments);
        path.validate()?;
        Ok(path)
    }

    #[must_use]
    /// Returns true when this path selects the whole document.
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    /// Returns the path segments.
    pub fn segments(&self) -> &[JsonPathSegment] {
        &self.0
    }

    fn validate(&self) -> Result<(), EngineError> {
        if self.0.len() > MAX_PATH_SEGMENTS {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_path_too_long",
                "JSON path has too many segments",
            ));
        }
        for segment in &self.0 {
            if let JsonPathSegment::Key(key) = segment {
                validate_component(
                    key,
                    MAX_DOCUMENT_ID_BYTES,
                    "invalid_argument.engine.json_path",
                    "JSON path key",
                )?;
            }
        }
        Ok(())
    }
}

impl Default for JsonPath {
    fn default() -> Self {
        Self::root()
    }
}

impl FromStr for JsonPath {
    type Err = EngineError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() || input == "$" {
            return Ok(Self::root());
        }
        let mut text = input;
        if let Some(rest) = text.strip_prefix("$.") {
            text = rest;
        } else if let Some(rest) = text.strip_prefix('$') {
            text = rest;
        }
        // A leading dot after the root means a doubled dot (`$..`), i.e.
        // recursive descent — which is unsupported. Reject it rather than
        // silently collapsing `$..b` to the top-level key `b`, matching the
        // rejection of a doubled dot mid-path.
        if text.starts_with('.') {
            return Err(path_error(
                "recursive descent is not supported in JSON path",
            ));
        }
        if text.is_empty() {
            return Ok(Self::root());
        }

        let chars = text.chars().collect::<Vec<_>>();
        let mut index = 0;
        let mut segments = Vec::new();
        while index < chars.len() {
            match chars[index] {
                '.' => {
                    index += 1;
                    if index >= chars.len() || chars[index] == '.' {
                        return Err(path_error("empty key in JSON path"));
                    }
                }
                '[' => {
                    parse_bracket_segment(&chars, &mut index, &mut segments)?;
                    continue;
                }
                _ => {}
            }

            if index >= chars.len() {
                break;
            }
            if chars[index] == '[' {
                continue;
            }
            let start = index;
            while index < chars.len()
                && (chars[index].is_alphanumeric() || chars[index] == '_' || chars[index] == '-')
            {
                index += 1;
            }
            if start == index {
                return Err(path_error(
                    "unsupported character in JSON path; use bracket notation \
                     (e.g. [\"my.key\"]) to address keys with dots, spaces, or \
                     other characters outside [A-Za-z0-9_-]",
                ));
            }
            segments.push(JsonPathSegment::Key(chars[start..index].iter().collect()));
        }
        Self::from_segments(segments)
    }
}

impl<'de> Deserialize<'de> for JsonPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum WirePath {
            Text(String),
            Segments(Vec<JsonPathSegment>),
        }

        match WirePath::deserialize(deserializer)? {
            WirePath::Text(path) => path.parse().map_err(serde::de::Error::custom),
            WirePath::Segments(segments) => {
                Self::from_segments(segments).map_err(serde::de::Error::custom)
            }
        }
    }
}

impl fmt::Display for JsonPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            return formatter.write_str("$");
        }
        let mut output = String::new();
        for segment in &self.0 {
            match segment {
                JsonPathSegment::Key(key) => {
                    if key.chars().all(|character| {
                        character.is_alphanumeric() || character == '_' || character == '-'
                    }) {
                        if !output.is_empty() {
                            output.push('.');
                        }
                        output.push_str(key);
                    } else {
                        output.push_str("[\"");
                        output.push_str(&key.replace('\\', "\\\\").replace('"', "\\\""));
                        output.push_str("\"]");
                    }
                }
                JsonPathSegment::Index(index) => {
                    output.push('[');
                    output.push_str(&index.to_string());
                    output.push(']');
                }
            }
        }
        formatter.write_str(&output)
    }
}

/// JSON secondary index kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonIndexType {
    /// Numeric field index.
    Numeric,
    /// Exact tag/string field index.
    Tag,
    /// Lowercase text field index.
    Text,
}

/// JSON secondary index definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonIndexDefinition {
    name: JsonIndexName,
    space: ProductSpace,
    field_path: JsonPath,
    index_type: JsonIndexType,
    created_version: u64,
    created_timestamp: u64,
}

impl JsonIndexDefinition {
    pub(crate) const fn new(
        name: JsonIndexName,
        space: ProductSpace,
        field_path: JsonPath,
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

    #[must_use]
    /// Returns the index name.
    pub const fn name(&self) -> &JsonIndexName {
        &self.name
    }

    #[must_use]
    /// Returns the product space.
    pub const fn space(&self) -> &ProductSpace {
        &self.space
    }

    #[must_use]
    /// Returns the indexed field path.
    pub const fn field_path(&self) -> &JsonPath {
        &self.field_path
    }

    #[must_use]
    /// Returns the index type.
    pub const fn index_type(&self) -> JsonIndexType {
        self.index_type
    }

    #[must_use]
    /// Returns the creation commit version encoded in metadata.
    pub const fn created_version(&self) -> u64 {
        self.created_version
    }

    #[must_use]
    /// Returns the creation commit timestamp encoded in metadata.
    pub const fn created_timestamp(&self) -> u64 {
        self.created_timestamp
    }
}

/// JSON batch set entry.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonSetEntry {
    id: JsonDocumentId,
    path: JsonPath,
    value: JsonValue,
}

impl JsonSetEntry {
    /// Creates one batch set entry.
    pub const fn new(id: JsonDocumentId, path: JsonPath, value: JsonValue) -> Self {
        Self { id, path, value }
    }

    pub(crate) const fn id(&self) -> &JsonDocumentId {
        &self.id
    }

    pub(crate) const fn path(&self) -> &JsonPath {
        &self.path
    }

    pub(crate) const fn value(&self) -> &JsonValue {
        &self.value
    }
}

/// JSON batch get entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonGetEntry {
    id: JsonDocumentId,
    path: JsonPath,
}

impl JsonGetEntry {
    /// Creates one batch get entry.
    pub const fn new(id: JsonDocumentId, path: JsonPath) -> Self {
        Self { id, path }
    }

    pub(crate) const fn id(&self) -> &JsonDocumentId {
        &self.id
    }

    pub(crate) const fn path(&self) -> &JsonPath {
        &self.path
    }
}

pub(crate) fn get_at_path(value: &JsonValue, path: &JsonPath) -> Option<JsonValue> {
    let mut current = value.as_inner();
    for segment in path.segments() {
        match (segment, current) {
            (JsonPathSegment::Key(key), Value::Object(object)) => current = object.get(key)?,
            (JsonPathSegment::Index(index), Value::Array(array)) => current = array.get(*index)?,
            _ => return None,
        }
    }
    Some(JsonValue::from_stored(current.clone()))
}

pub(crate) fn set_at_path(
    value: &mut JsonValue,
    path: &JsonPath,
    replacement: JsonValue,
    create_missing: bool,
) -> Result<(), EngineError> {
    if path.is_root() {
        *value = replacement;
        value.validate()?;
        return Ok(());
    }
    set_value_at_path(
        value.as_inner_mut_for_path(),
        path.segments(),
        replacement.into_inner(),
        create_missing,
    )?;
    value.validate()
}

pub(crate) fn delete_at_path(value: &mut JsonValue, path: &JsonPath) -> Result<bool, EngineError> {
    if path.is_root() {
        return Ok(false);
    }
    let deleted = delete_value_at_path(value.as_inner_mut_for_path(), path.segments())?;
    if deleted {
        value.validate()?;
    }
    Ok(deleted)
}

impl JsonValue {
    fn as_inner_mut_for_path(&mut self) -> &mut Value {
        &mut self.0
    }
}

fn parse_bracket_segment(
    chars: &[char],
    index: &mut usize,
    segments: &mut Vec<JsonPathSegment>,
) -> Result<(), EngineError> {
    *index += 1;
    if *index < chars.len() && (chars[*index] == '"' || chars[*index] == '\'') {
        // Quoted-key bracket notation addresses arbitrary keys — dots, spaces,
        // emoji, `$`/`@`, etc. Both quote styles are accepted (standard JSONPath);
        // `\<quote>` and `\\` are the only escapes.
        let quote = chars[*index];
        *index += 1;
        let mut key = String::new();
        while *index < chars.len() && chars[*index] != quote {
            if chars[*index] == '\\' && *index + 1 < chars.len() {
                let next = chars[*index + 1];
                if next == quote || next == '\\' {
                    key.push(next);
                    *index += 2;
                    continue;
                }
            }
            key.push(chars[*index]);
            *index += 1;
        }
        if *index >= chars.len() || chars[*index] != quote {
            return Err(path_error("unclosed quoted key in JSON path"));
        }
        *index += 1;
        if *index >= chars.len() || chars[*index] != ']' {
            return Err(path_error("unclosed bracket in JSON path"));
        }
        *index += 1;
        if key.is_empty() {
            return Err(path_error("empty key in JSON path"));
        }
        segments.push(JsonPathSegment::Key(key));
        return Ok(());
    }

    let number_start = *index;
    while *index < chars.len() && chars[*index] != ']' {
        *index += 1;
    }
    if *index >= chars.len() {
        return Err(path_error("unclosed bracket in JSON path"));
    }
    let index_text = chars[number_start..*index].iter().collect::<String>();
    *index += 1;
    if index_text.is_empty() {
        return Err(path_error("empty array index in JSON path"));
    }
    let parsed = index_text
        .parse::<usize>()
        .map_err(|_| path_error("invalid array index in JSON path"))?;
    segments.push(JsonPathSegment::Index(parsed));
    Ok(())
}

fn set_value_at_path(
    current: &mut Value,
    segments: &[JsonPathSegment],
    replacement: Value,
    create_missing: bool,
) -> Result<(), EngineError> {
    let Some((first, rest)) = segments.split_first() else {
        *current = replacement;
        return Ok(());
    };
    if rest.is_empty() {
        return set_terminal_value(current, first, replacement, create_missing);
    }
    match first {
        JsonPathSegment::Key(key) => {
            if current.is_null() && create_missing {
                *current = Value::Object(Map::new());
            }
            let Value::Object(object) = current else {
                return Err(type_mismatch("object"));
            };
            if !object.contains_key(key) {
                if create_missing {
                    object.insert(key.clone(), Value::Object(Map::new()));
                } else {
                    return Err(path_not_found());
                }
            }
            let child = object.get_mut(key).expect("inserted child");
            set_value_at_path(child, rest, replacement, create_missing)
        }
        JsonPathSegment::Index(index) => {
            let Value::Array(array) = current else {
                return Err(type_mismatch("array"));
            };
            let Some(child) = array.get_mut(*index) else {
                return Err(path_not_found());
            };
            set_value_at_path(child, rest, replacement, create_missing)
        }
    }
}

fn set_terminal_value(
    current: &mut Value,
    segment: &JsonPathSegment,
    replacement: Value,
    create_missing: bool,
) -> Result<(), EngineError> {
    match segment {
        JsonPathSegment::Key(key) => {
            if current.is_null() && create_missing {
                *current = Value::Object(Map::new());
            }
            let Value::Object(object) = current else {
                return Err(type_mismatch("object"));
            };
            if !create_missing && !object.contains_key(key) {
                return Err(path_not_found());
            }
            object.insert(key.clone(), replacement);
            Ok(())
        }
        JsonPathSegment::Index(index) => {
            let Value::Array(array) = current else {
                return Err(type_mismatch("array"));
            };
            let Some(slot) = array.get_mut(*index) else {
                return Err(path_not_found());
            };
            *slot = replacement;
            Ok(())
        }
    }
}

fn delete_value_at_path(
    current: &mut Value,
    segments: &[JsonPathSegment],
) -> Result<bool, EngineError> {
    let Some((first, rest)) = segments.split_first() else {
        return Ok(false);
    };
    if rest.is_empty() {
        return match (first, current) {
            (JsonPathSegment::Key(key), Value::Object(object)) => Ok(object.remove(key).is_some()),
            (JsonPathSegment::Index(index), Value::Array(array)) => {
                if *index < array.len() {
                    array.remove(*index);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            (JsonPathSegment::Key(_), _) => Err(type_mismatch("object")),
            (JsonPathSegment::Index(_), _) => Err(type_mismatch("array")),
        };
    }
    match (first, current) {
        (JsonPathSegment::Key(key), Value::Object(object)) => {
            let Some(child) = object.get_mut(key) else {
                return Ok(false);
            };
            delete_value_at_path(child, rest)
        }
        (JsonPathSegment::Index(index), Value::Array(array)) => {
            let Some(child) = array.get_mut(*index) else {
                return Ok(false);
            };
            delete_value_at_path(child, rest)
        }
        (JsonPathSegment::Key(_), _) => Err(type_mismatch("object")),
        (JsonPathSegment::Index(_), _) => Err(type_mismatch("array")),
    }
}

fn validate_component(
    value: &str,
    max_bytes: usize,
    code: &'static str,
    label: &'static str,
) -> Result<(), EngineError> {
    if value.is_empty() {
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
    if value.bytes().any(|byte| byte == 0 || byte == b'\n') {
        return Err(EngineError::invalid_input(
            code,
            format!("{label} contains an unsupported control byte"),
        ));
    }
    Ok(())
}

/// Returns true if `value` nests deeper than `max` container levels, using an
/// explicit stack so an arbitrarily deep value is detected without recursion.
///
/// Matches the previous recursive definition where each array/object counts as
/// one level and scalars as zero; the root container is level 1.
fn exceeds_nesting_depth(value: &Value, max: usize) -> bool {
    let mut stack: Vec<(&Value, usize)> = vec![(value, 0)];
    while let Some((node, containers_above)) = stack.pop() {
        match node {
            Value::Array(array) => {
                if containers_above + 1 > max {
                    return true;
                }
                stack.extend(array.iter().map(|child| (child, containers_above + 1)));
            }
            Value::Object(object) => {
                if containers_above + 1 > max {
                    return true;
                }
                stack.extend(object.values().map(|child| (child, containers_above + 1)));
            }
            _ => {}
        }
    }
    false
}

fn max_array_size(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => 0,
        Value::Array(array) => array
            .len()
            .max(array.iter().map(max_array_size).max().unwrap_or(0)),
        Value::Object(object) => object.values().map(max_array_size).max().unwrap_or(0),
    }
}

fn path_error(message: impl Into<String>) -> EngineError {
    EngineError::invalid_input("invalid_argument.engine.json_path", message)
}

fn path_not_found() -> EngineError {
    EngineError::invalid_input(
        "invalid_argument.engine.json_path_not_found",
        "JSON path not found",
    )
}

fn type_mismatch(expected: &'static str) -> EngineError {
    EngineError::invalid_input(
        "invalid_argument.engine.json_path_type",
        format!("JSON path expected {expected}"),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use serde_json::Value;

    use super::{
        delete_at_path, get_at_path, set_at_path, JsonDocumentId, JsonIndexName, JsonPath,
        JsonPathSegment, JsonValue, MAX_ARRAY_SIZE, MAX_DOCUMENT_BYTES, MAX_DOCUMENT_ID_BYTES,
        MAX_PATH_SEGMENTS,
    };
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn document_ids_and_index_names_validate_boundaries() {
        assert_eq!(
            JsonDocumentId::new("用户").expect("UTF-8 id").as_str(),
            "用户"
        );
        assert!(
            JsonDocumentId::new("a").expect("valid id")
                < JsonDocumentId::new("b").expect("valid id")
        );

        let empty = JsonDocumentId::new("").expect_err("empty id rejected");
        assert_eq!(empty.class(), EngineErrorClass::InvalidInput);

        let too_long =
            JsonDocumentId::new("x".repeat(MAX_DOCUMENT_ID_BYTES + 1)).expect_err("too long");
        assert_eq!(too_long.code(), "invalid_argument.engine.json_document_id");

        let control = JsonDocumentId::new("bad\nid").expect_err("control byte rejected");
        assert_eq!(control.class(), EngineErrorClass::InvalidInput);

        let reserved = JsonIndexName::new("_reserved").expect_err("reserved index rejected");
        assert_eq!(
            reserved.code(),
            "invalid_argument.engine.json_index_name_reserved"
        );
    }

    #[test]
    fn json_value_validation_covers_supported_shapes_and_limits() {
        for value in [
            json!(null),
            json!(true),
            json!(42),
            json!("text"),
            json!([1, 2, 3]),
            json!({"a": 1}),
        ] {
            JsonValue::new(value).expect("supported value");
        }

        let too_large =
            JsonValue::new(json!("x".repeat(MAX_DOCUMENT_BYTES))).expect_err("too large");
        assert_eq!(
            too_large.code(),
            "invalid_argument.engine.json_document_too_large"
        );

        let mut deep = json!(null);
        for _ in 0..101 {
            deep = json!({ "nested": deep });
        }
        let too_deep = JsonValue::new(deep).expect_err("too deep");
        assert_eq!(
            too_deep.code(),
            "invalid_argument.engine.json_document_too_deep"
        );

        // A value far past the bound is rejected by the iterative pre-check
        // before any recursive serialization runs (finding U36). (A depth in
        // the tens of thousands is avoided here only because serde_json::Value's
        // own Drop is recursive, which is outside validate()'s control.)
        let mut very_deep = json!(null);
        for _ in 0..2_000 {
            very_deep = Value::Array(vec![very_deep]);
        }
        assert_eq!(
            JsonValue::new(very_deep).expect_err("far too deep").code(),
            "invalid_argument.engine.json_document_too_deep"
        );

        let too_wide = JsonValue::new(Value::Array(vec![Value::Null; MAX_ARRAY_SIZE + 1]))
            .expect_err("array too large");
        assert_eq!(
            too_wide.code(),
            "invalid_argument.engine.json_array_too_large"
        );
    }

    #[test]
    fn path_parser_accepts_root_dot_and_bracket_forms() {
        assert!("".parse::<JsonPath>().expect("root").is_root());
        assert!("$".parse::<JsonPath>().expect("root").is_root());
        assert_eq!(
            "$.user[0][\"display.name\"]"
                .parse::<JsonPath>()
                .expect("path")
                .segments()
                .len(),
            3
        );
    }

    #[test]
    fn path_parser_accepts_single_quoted_bracket_keys() {
        // Single-quote bracket notation addresses arbitrary keys just like the
        // double-quote form, matching standard JSONPath. A key that dot notation
        // cannot express (emoji, dot, embedded double quote) round-trips.
        for (path, key) in [
            ("$['k\u{1f389}']", "k\u{1f389}"),
            ("$['a.b']", "a.b"),
            ("$['has \"double\"']", "has \"double\""),
            ("$['esc\\'aped']", "esc'aped"),
            // A backslash before a non-escape character is preserved literally
            // (only `\'` and `\\` are escapes), not consumed with the next char.
            ("$['a\\b']", "a\\b"),
        ] {
            let parsed = path
                .parse::<JsonPath>()
                .expect("single-quoted bracket path parses");
            assert_eq!(
                parsed.segments(),
                &[JsonPathSegment::Key(key.to_owned())],
                "{path}"
            );
        }
        // An unterminated single-quoted key is rejected like the double-quote form.
        assert_eq!(
            "$['open"
                .parse::<JsonPath>()
                .expect_err("unterminated")
                .code(),
            "invalid_argument.engine.json_path"
        );
        // A trailing backslash at the end of input must not read past the buffer
        // while looking for the escaped character; it is an unterminated key.
        assert_eq!(
            "$['a\\"
                .parse::<JsonPath>()
                .expect_err("trailing backslash is unterminated")
                .code(),
            "invalid_argument.engine.json_path"
        );
        // An opening bracket at the very end of input is an unclosed bracket, not
        // a read past the buffer while probing for a quote.
        assert_eq!(
            "$[".parse::<JsonPath>()
                .expect_err("bracket at end is unclosed")
                .code(),
            "invalid_argument.engine.json_path"
        );
    }

    #[test]
    fn path_parser_rejects_malformed_and_oversized_paths() {
        for input in ["user..name", "items[]", "items[abc]", "[\"unterminated\""] {
            let error = input.parse::<JsonPath>().expect_err("invalid path");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        }

        let too_long = JsonPath::from_segments(vec![
            JsonPathSegment::Key("x".to_owned());
            MAX_PATH_SEGMENTS + 1
        ])
        .expect_err("too many segments");
        assert_eq!(
            too_long.code(),
            "invalid_argument.engine.json_path_too_long"
        );
    }

    #[test]
    fn path_parser_rejects_recursive_descent_instead_of_collapsing_it() {
        // A leading `$..` must not silently collapse to a top-level key
        // (`$..b` -> `b`), which returns wrong results on nested keys; reject it
        // with the same typed error as a doubled dot mid-path (`$.a..b`).
        for input in ["$..b", "$..top", "$..", "$.a..b", ".b"] {
            let error = input
                .parse::<JsonPath>()
                .expect_err("recursive descent is rejected");
            assert_eq!(error.code(), "invalid_argument.engine.json_path");
        }

        // Ordinary single-target paths are unaffected.
        assert_eq!(
            "$.a.b"
                .parse::<JsonPath>()
                .expect("valid path")
                .segments()
                .len(),
            2
        );
    }

    #[test]
    fn path_formatting_round_trips_deterministically() {
        let path = JsonPath::from_segments(vec![
            JsonPathSegment::Key("user".to_owned()),
            JsonPathSegment::Key("display.name".to_owned()),
            JsonPathSegment::Index(2),
        ])
        .expect("valid path");
        let text = path.to_string();
        assert_eq!(text, "user[\"display.name\"][2]");
        assert_eq!(text.parse::<JsonPath>().expect("round trip"), path);
    }

    #[test]
    fn path_helpers_mutate_one_deterministic_path() {
        let mut value =
            JsonValue::new(json!({"user": {"name": "a"}, "items": [1, 2]})).expect("valid value");
        let path = "user.name".parse::<JsonPath>().expect("path");
        set_at_path(
            &mut value,
            &path,
            JsonValue::new(json!("b")).expect("valid"),
            true,
        )
        .expect("set succeeds");
        assert_eq!(
            get_at_path(&value, &path).expect("value").as_inner(),
            &json!("b")
        );

        let deleted = delete_at_path(&mut value, &path).expect("delete succeeds");
        assert!(deleted);
        assert!(get_at_path(&value, &path).is_none());
    }
}
