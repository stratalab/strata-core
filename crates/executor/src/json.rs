//! Canonical JSON serialization for Strata types.
//!
//! This module provides special JSON handling for types that don't have
//! a standard JSON representation:
//!
//! | Type | JSON Representation |
//! |------|---------------------|
//! | Bytes | `{"$bytes": "<base64>"}` |
//! | NaN | `{"$f64": "NaN"}` |
//! | +Infinity | `{"$f64": "+Inf"}` |
//! | -Infinity | `{"$f64": "-Inf"}` |
//! | -0.0 | `{"$f64": "-0.0"}` |
//!
//! This ensures round-trip serialization preserves exact values.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use strata_core::Value;

/// Serialize a Value to canonical JSON.
///
/// Handles special cases:
/// - Bytes are encoded as `{"$bytes": "<base64>"}`
/// - Special floats (NaN, +/-Inf, -0.0) are encoded as `{"$f64": "..."}`
pub fn serialize_value<S>(value: &Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let json_value = value_to_json(value);
    json_value.serialize(serializer)
}

/// Deserialize a Value from canonical JSON.
///
/// Recognizes special encodings:
/// - `{"$bytes": "<base64>"}` -> Bytes
/// - `{"$f64": "..."}` -> Float with special value
pub fn deserialize_value<'de, D>(deserializer: D) -> Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let json_value = JsonValue::deserialize(deserializer)?;
    json_to_value(&json_value).map_err(de::Error::custom)
}

/// Convert a Value to a JSON value with special encoding.
pub fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Int(i) => JsonValue::Number((*i).into()),
        Value::Float(f) => float_to_json(*f),
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Bytes(b) => {
            let encoded = BASE64.encode(b);
            serde_json::json!({"$bytes": encoded})
        }
        Value::Array(arr) => {
            let items: Vec<JsonValue> = arr.iter().map(value_to_json).collect();
            JsonValue::Array(items)
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, JsonValue> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
    }
}

/// Convert a JSON value to a Value, recognizing special encodings.
pub fn json_to_value(json: &JsonValue) -> Result<Value, String> {
    match json {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err("Invalid number".to_string())
            }
        }
        JsonValue::String(s) => Ok(Value::String(s.clone())),
        JsonValue::Array(arr) => {
            let items: Result<Vec<Value>, String> = arr.iter().map(json_to_value).collect();
            Ok(Value::Array(items?))
        }
        JsonValue::Object(obj) => {
            // Check for special encodings
            if let Some(bytes_value) = obj.get("$bytes") {
                if obj.len() == 1 {
                    if let JsonValue::String(encoded) = bytes_value {
                        let decoded = BASE64
                            .decode(encoded)
                            .map_err(|e| format!("Invalid base64: {}", e))?;
                        return Ok(Value::Bytes(decoded));
                    }
                }
            }
            if let Some(float_value) = obj.get("$f64") {
                if obj.len() == 1 {
                    if let JsonValue::String(s) = float_value {
                        let f = json_special_float_from_str(s)?;
                        return Ok(Value::Float(f));
                    }
                }
            }

            // Regular object
            let map: Result<std::collections::HashMap<String, Value>, String> = obj
                .iter()
                .map(|(k, v)| json_to_value(v).map(|val| (k.clone(), val)))
                .collect();
            Ok(Value::Object(map?))
        }
    }
}

/// Convert a float to JSON, handling special values.
fn float_to_json(f: f64) -> JsonValue {
    if f.is_nan() {
        serde_json::json!({"$f64": "NaN"})
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            serde_json::json!({"$f64": "+Inf"})
        } else {
            serde_json::json!({"$f64": "-Inf"})
        }
    } else if f == 0.0 && f.is_sign_negative() {
        serde_json::json!({"$f64": "-0.0"})
    } else {
        // Regular float - use standard JSON number
        serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or_else(|| {
                // Fallback for edge cases (shouldn't happen with normal floats)
                serde_json::json!({"$f64": format!("{}", f)})
            })
    }
}

/// Parse a special float string.
fn json_special_float_from_str(s: &str) -> Result<f64, String> {
    match s {
        "NaN" => Ok(f64::NAN),
        "+Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        "-0.0" => Ok(-0.0_f64),
        other => other
            .parse::<f64>()
            .map_err(|e| format!("Invalid float: {}", e)),
    }
}

/// A wrapper for Value that uses canonical JSON serialization.
///
/// Use this when you need to serialize a Value with special handling.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalValue(pub Value);

impl Serialize for CanonicalValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_value(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for CanonicalValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_value(deserializer).map(CanonicalValue)
    }
}

impl From<Value> for CanonicalValue {
    fn from(v: Value) -> Self {
        CanonicalValue(v)
    }
}

impl From<CanonicalValue> for Value {
    fn from(v: CanonicalValue) -> Self {
        v.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_round_trip() {
        let original = Value::Bytes(vec![1, 2, 3, 255, 0]);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_bytes_format() {
        let value = Value::Bytes(vec![1, 2, 3]);
        let json = value_to_json(&value);
        assert!(json.is_object());
        assert!(json.get("$bytes").is_some());
    }

    #[test]
    fn test_nan_round_trip() {
        let original = Value::Float(f64::NAN);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        match restored {
            Value::Float(f) => assert!(f.is_nan()),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_positive_infinity_round_trip() {
        let original = Value::Float(f64::INFINITY);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        assert_eq!(restored, Value::Float(f64::INFINITY));
    }

    #[test]
    fn test_negative_infinity_round_trip() {
        let original = Value::Float(f64::NEG_INFINITY);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        assert_eq!(restored, Value::Float(f64::NEG_INFINITY));
    }

    #[test]
    fn test_negative_zero_round_trip() {
        let original = Value::Float(-0.0_f64);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        match restored {
            Value::Float(f) => {
                assert_eq!(f, 0.0);
                assert!(f.is_sign_negative());
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_regular_float_round_trip() {
        let original = Value::Float(3.125);
        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();
        assert_eq!(restored, Value::Float(3.125));
    }

    #[test]
    fn test_complex_value_round_trip() {
        let original = Value::Object(
            [
                ("name".to_string(), Value::String("test".to_string())),
                ("count".to_string(), Value::Int(42)),
                ("data".to_string(), Value::Bytes(vec![1, 2, 3])),
                ("nan".to_string(), Value::Float(f64::NAN)),
                (
                    "nested".to_string(),
                    Value::Array(vec![Value::Float(f64::INFINITY), Value::Float(-0.0)]),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let json = value_to_json(&original);
        let restored = json_to_value(&json).unwrap();

        // Check specific fields since NaN != NaN
        match (&original, &restored) {
            (Value::Object(o), Value::Object(r)) => {
                assert_eq!(o.get("name"), r.get("name"));
                assert_eq!(o.get("count"), r.get("count"));
                assert_eq!(o.get("data"), r.get("data"));

                // Check NaN specifically
                match (o.get("nan"), r.get("nan")) {
                    (Some(Value::Float(f1)), Some(Value::Float(f2))) => {
                        assert!(f1.is_nan() && f2.is_nan());
                    }
                    _ => panic!("Expected NaN"),
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_canonical_value_serde() {
        let value = CanonicalValue(Value::Bytes(vec![1, 2, 3]));
        let json = serde_json::to_string(&value).unwrap();
        let restored: CanonicalValue = serde_json::from_str(&json).unwrap();
        assert_eq!(value, restored);
    }
}
