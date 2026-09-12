//! Event input types.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::diagnostics::EngineError;

const MAX_EVENT_TYPE_BYTES: usize = 256;
const MAX_EVENT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const EVENT_HASH_BYTES: usize = 32;

/// Event type filter over the global event log.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct EventType(String);

impl EventType {
    /// Creates a validated event type.
    pub fn new(event_type: impl Into<String>) -> Result<Self, EngineError> {
        let event_type = event_type.into();
        if event_type.is_empty() || event_type.trim().is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_type",
                "event type must not be empty",
            ));
        }
        if event_type.len() > MAX_EVENT_TYPE_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_type",
                "event type exceeds the maximum length",
            ));
        }
        if event_type.bytes().any(|byte| byte == 0) {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_type",
                "event type contains an unsupported control byte",
            ));
        }
        Ok(Self(event_type))
    }

    #[must_use]
    /// Returns the event type text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for EventType {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EventType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Event payload wrapper.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EventPayload(Value);

impl EventPayload {
    /// Creates a validated event payload.
    pub fn new(value: Value) -> Result<Self, EngineError> {
        if !value.is_object() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_payload",
                "event payload must be a JSON object",
            ));
        }
        if contains_non_finite_float(&value) {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_payload",
                "event payload must not contain non-finite floats",
            ));
        }
        let size = serde_json::to_vec(&value)
            .map_err(|error| {
                EngineError::invalid_input(
                    "invalid_argument.engine.event_payload",
                    format!("event payload cannot be encoded: {error}"),
                )
            })?
            .len();
        if size > MAX_EVENT_PAYLOAD_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_payload_too_large",
                "event payload exceeds the maximum encoded size",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    /// Returns the wrapped JSON value.
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

    pub(crate) fn from_stored(value: Value) -> Result<Self, EngineError> {
        Self::new(value)
    }
}

impl TryFrom<Value> for EventPayload {
    type Error = EngineError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for EventPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Event sequence number.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct EventSequence(u64);

impl EventSequence {
    #[must_use]
    /// Creates an event sequence.
    pub const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    #[must_use]
    /// Returns the raw sequence number.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for EventSequence {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<EventSequence> for u64 {
    fn from(value: EventSequence) -> Self {
        value.as_u64()
    }
}

/// Event range direction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventRangeDirection {
    /// Increasing sequence or timestamp order.
    #[default]
    Forward,
    /// Decreasing sequence or timestamp order.
    Reverse,
}

/// One event batch append request item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBatchAppendEntry {
    event_type: String,
    payload: Value,
}

impl EventBatchAppendEntry {
    /// Creates a batch append entry from validated values.
    #[must_use]
    pub fn new(event_type: EventType, payload: EventPayload) -> Self {
        let EventType(event_type) = event_type;
        Self {
            event_type,
            payload: payload.into_inner(),
        }
    }

    /// Creates a batch append entry from wire-shaped values.
    #[must_use]
    pub fn from_raw(event_type: impl Into<String>, payload: Value) -> Self {
        Self {
            event_type: event_type.into(),
            payload,
        }
    }

    pub(crate) fn validate(&self) -> Result<(EventType, EventPayload), EngineError> {
        let event_type = EventType::new(self.event_type.clone())?;
        let payload = EventPayload::new(self.payload.clone())?;
        Ok((event_type, payload))
    }

    #[must_use]
    /// Returns the event type text.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    #[must_use]
    /// Returns the event payload JSON value.
    pub const fn payload(&self) -> &Value {
        &self.payload
    }
}

pub(crate) type EventHash = [u8; EVENT_HASH_BYTES];

fn contains_non_finite_float(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.as_f64().is_some_and(|value| !value.is_finite()),
        Value::Array(values) => values.iter().any(contains_non_finite_float),
        Value::Object(map) => map.values().any(contains_non_finite_float),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{EventBatchAppendEntry, EventPayload, EventType};
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn event_type_validation_rejects_empty_or_oversized_values() {
        for rejected in ["", " \t "] {
            let error = EventType::new(rejected).expect_err("event type rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.engine.event_type");
        }

        let error = EventType::new("e".repeat(257)).expect_err("oversized event type rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.event_type");
    }

    #[test]
    fn event_type_validation_accepts_boundary_and_rejects_control_bytes() {
        let accepted = EventType::new("e".repeat(256)).expect("boundary type accepted");
        assert_eq!(accepted.as_str().len(), 256);

        let error = EventType::new("bad\0type").expect_err("control byte rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.event_type");
    }

    #[test]
    fn event_payload_validation_requires_object_root() {
        for rejected in [
            json!(null),
            json!(1),
            json!("text"),
            json!(true),
            json!([1]),
        ] {
            let error = EventPayload::new(rejected).expect_err("payload rejected");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.engine.event_payload");
        }

        EventPayload::new(json!({})).expect("empty object accepted");
        EventPayload::new(json!({"nested": [true, 1, "two"]})).expect("nested object accepted");
    }

    #[test]
    fn batch_entries_validate_positionally_inside_service() {
        let valid = EventBatchAppendEntry::from_raw("audit.recorded", json!({"ok": true}));
        let (event_type, payload) = valid.validate().expect("valid entry accepted");
        assert_eq!(event_type.as_str(), "audit.recorded");
        assert_eq!(payload.as_inner(), &json!({"ok": true}));

        let bad_type = EventBatchAppendEntry::from_raw("", json!({}));
        assert_eq!(
            bad_type.validate().expect_err("type rejected").code(),
            "invalid_argument.engine.event_type"
        );

        let bad_payload = EventBatchAppendEntry::from_raw("audit.recorded", json!([]));
        assert_eq!(
            bad_payload.validate().expect_err("payload rejected").code(),
            "invalid_argument.engine.event_payload"
        );
    }
}
