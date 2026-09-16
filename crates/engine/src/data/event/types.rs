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
    ///
    /// The type must be non-empty once trimmed, at most **256 bytes**, and free
    /// of NUL bytes. Each refusal carries `invalid_argument.engine.event_type`.
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
    ///
    /// The payload must be a JSON **object** — an array, string, number,
    /// boolean or `null` root is refused with
    /// `invalid_argument.engine.event_payload`. An empty object is valid.
    /// Encoded, it must not exceed **16 MiB**
    /// (`invalid_argument.engine.event_payload_too_large`).
    ///
    /// # A non-finite float never reaches this constructor
    ///
    /// `NaN` and `±Inf` are **not** refused here, because they cannot arrive.
    /// `serde_json::Value` has no representation for a non-finite number:
    /// `Number::from_f64` returns `None`, so `json!({ "vx": f64::NAN })` builds
    /// `{"vx": null}` in the caller's own crate. By the time the value is
    /// passed here the float is already gone, and the payload is accepted with
    /// a `null` in its place.
    ///
    /// A caller assembling a payload from float data must therefore check
    /// finiteness *before* building the `Value` — the engine cannot tell a
    /// coerced `NaN` from a `null` the caller meant to write. Strata's own
    /// float-bearing entry points check at the boundary where the value is
    /// still a float, not a `Value`: Arrow import refuses a non-finite cell
    /// with `invalid_argument.executor.arrow_non_finite_float`, and
    /// [`VectorEmbedding::from_wire`] refuses one with
    /// `invalid_argument.engine.vector_embedding`.
    ///
    /// [`VectorEmbedding::from_wire`]: crate::VectorEmbedding::from_wire
    pub fn new(value: Value) -> Result<Self, EngineError> {
        if !value.is_object() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.event_payload",
                "event payload must be a JSON object",
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

#[cfg(test)]
mod tests {
    use serde::de::value::{Error as ValueError, F64Deserializer};
    use serde::Deserialize;
    use serde_json::{json, Number, Value};

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

    /// The reason `EventPayload::new` carries no finite-float rule: a
    /// `serde_json::Value` cannot hold a non-finite number, by any route.
    ///
    /// This is the guard for the rustdoc on `EventPayload::new`. If
    /// `serde_json` or this workspace's feature set ever makes a `Number` hold
    /// a non-finite value, this test goes red — and the constructor needs a
    /// finiteness check back, because the rustdoc's claim would no longer hold.
    #[test]
    fn serde_json_cannot_represent_a_non_finite_float() {
        for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            // Constructed: `Number::from_f64` refuses, and the `json!` macro
            // and `Value::from` fall back to null rather than erroring.
            assert!(Number::from_f64(non_finite).is_none());
            assert_eq!(Value::from(non_finite), Value::Null);

            // Deserialized: a binary codec handing serde a raw f64 on
            // read-back cannot produce one either.
            let number: Result<Number, ValueError> =
                Number::deserialize(F64Deserializer::<ValueError>::new(non_finite));
            assert!(number.is_err());
            let value: Value = Value::deserialize(F64Deserializer::<ValueError>::new(non_finite))
                .expect("a Value deserializes");
            assert_eq!(value, Value::Null);
        }

        // Parsed: JSON text has no non-finite literal, and an out-of-range
        // exponent is a parse error rather than an infinity.
        for text in [r#"{"vx": NaN}"#, r#"{"vx": Infinity}"#, r#"{"vx": 1e400}"#] {
            assert!(serde_json::from_str::<Value>(text).is_err());
        }
    }

    /// The behaviour the rustdoc warns about: a `NaN` is coerced to `null` in
    /// the caller's crate, and the payload is accepted with the number gone.
    #[test]
    fn a_non_finite_float_is_accepted_as_null_not_refused() {
        let payload = EventPayload::new(json!({"vx": f64::NAN, "vy": f64::INFINITY}))
            .expect("a coerced payload is accepted, not refused");
        assert_eq!(payload.as_inner(), &json!({"vx": null, "vy": null}));
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
