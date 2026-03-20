//! Value types for Strata
//!
//! This module defines:
//! - Value: Unified enum for all primitive data types
//!
//! ## Canonical Value Model (Frozen)
//!
//! The Value enum has exactly 8 variants, matching the core contract:
//! - Null, Bool, Int, Float, String, Bytes, Array, Object
//!
//! ### Type Rules (VAL-1 to VAL-5)
//!
//! - **VAL-1**: Eight types only
//! - **VAL-2**: No implicit type coercions
//! - **VAL-3**: `Int(1) != Float(1.0)` - different types are NEVER equal
//! - **VAL-4**: `Bytes` are not `String`
//! - **VAL-5**: Float uses IEEE-754 equality: `NaN != NaN`, `-0.0 == 0.0`
//!
//! ## Migration Note
//!
//! - `Timestamp` is now in `contract::Timestamp` (microseconds, not seconds)
//! - `VersionedValue` is now `contract::Versioned<Value>`
//!
//! Import from crate root: `use strata_core::{Timestamp, VersionedValue, Version};`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Canonical Strata value type for all API surfaces
///
/// This enum represents the 8 canonical value types in the Strata data model.
/// All SDKs must map to this model. JSON is a strict subset.
///
/// ## Type Equality
///
/// Different types are NEVER equal, even if they contain the same "value":
/// - `Int(1) != Float(1.0)`
/// - `Bytes(b"hello") != String("hello")`
///
/// Float equality follows IEEE-754 semantics:
/// - `NaN != NaN`
/// - `-0.0 == 0.0`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating point (IEEE-754)
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Raw bytes.
    ///
    /// **JSON roundtrip note**: When a `Value::Bytes` is serialized to JSON
    /// (e.g., via `serde_json`), the bytes are base64-encoded into a JSON string.
    /// Deserializing that JSON string back produces `Value::String`, not
    /// `Value::Bytes`. This means `Bytes` values do not survive a JSON roundtrip
    /// with their original type intact. Use binary serialization (e.g., bincode,
    /// MessagePack) for lossless `Bytes` roundtrips.
    Bytes(Vec<u8>),
    /// Array of values
    ///
    /// Boxed to shrink `Value` from ~56 to ~24 bytes. At 128M entries
    /// this saves ~3.8 GB of RAM. The `Box` is transparent to serde.
    Array(Box<Vec<Value>>),
    /// Object with string keys (JSON object)
    ///
    /// Boxed to shrink `Value` from ~56 to ~24 bytes. See `Array` above.
    Object(Box<HashMap<String, Value>>),
}

// Custom PartialEq implementation for IEEE-754 float semantics
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            // IEEE-754: NaN != NaN, -0.0 == 0.0
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, v)| b.get(k) == Some(v))
            }
            // Different types are NEVER equal (VAL-3)
            _ => false,
        }
    }
}

impl Value {
    /// Create an Array value (convenience constructor that handles boxing)
    pub fn array(v: Vec<Value>) -> Self {
        Value::Array(Box::new(v))
    }

    /// Create an Object value (convenience constructor that handles boxing)
    pub fn object(m: HashMap<String, Value>) -> Self {
        Value::Object(Box::new(m))
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int",
            Value::Float(_) => "Float",
            Value::String(_) => "String",
            Value::Bytes(_) => "Bytes",
            Value::Array(_) => "Array",
            Value::Object(_) => "Object",
        }
    }

    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if this is a boolean value
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Check if this is an integer value
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Check if this is a float value
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Check if this is a string value
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Check if this is a bytes value
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    /// Check if this is an array value
    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    /// Check if this is an object value
    pub fn is_object(&self) -> bool {
        matches!(self, Value::Object(_))
    }

    /// Get as bool if this is a Bool value
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as i64 if this is an Int value
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as f64 if this is a Float value
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Get as &str if this is a String value
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as &[u8] if this is a Bytes value
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Get as &[Value] if this is an Array value
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Get as &HashMap if this is an Object value
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }
}

// ============================================================================
// From implementations for ergonomic API usage
// ============================================================================

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Value::Float(f as f64)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl From<&[u8]> for Value {
    fn from(b: &[u8]) -> Self {
        Value::Bytes(b.to_vec())
    }
}

impl From<Vec<Value>> for Value {
    fn from(a: Vec<Value>) -> Self {
        Value::array(a)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(o: HashMap<String, Value>) -> Self {
        Value::object(o)
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

// ============================================================================
// serde_json interop for ergonomic JSON construction
// ============================================================================

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    // Fallback for u64 that doesn't fit in i64
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::array(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::object(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => {
                // Encode bytes as base64 string for JSON compatibility
                serde_json::Value::String(base64_encode_bytes(&b))
            }
            Value::Array(arr) => {
                serde_json::Value::Array((*arr).into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(obj) => serde_json::Value::Object(
                (*obj)
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

/// Simple base64 encoding for bytes (no external dependency)
///
/// Public so that other crates (e.g., engine canonical JSON serializer) can
/// produce the same encoding without duplicating the implementation.
pub fn base64_encode_bytes(data: &[u8]) -> String {
    use std::fmt::Write;
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        let _ = write!(result, "{}", ALPHABET[(b0 >> 2) & 0x3F] as char);
        let _ = write!(
            result,
            "{}",
            ALPHABET[((b0 << 4) | (b1 >> 4)) & 0x3F] as char
        );

        if chunk.len() > 1 {
            let _ = write!(
                result,
                "{}",
                ALPHABET[((b1 << 2) | (b2 >> 6)) & 0x3F] as char
            );
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            let _ = write!(result, "{}", ALPHABET[b2 & 0x3F] as char);
        } else {
            result.push('=');
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for Value enum variants

    #[test]
    fn test_value_null() {
        let value = Value::Null;
        assert!(matches!(value, Value::Null));
        assert!(value.is_null());
    }

    #[test]
    fn test_value_bool() {
        let value_true = Value::Bool(true);
        let value_false = Value::Bool(false);

        assert!(matches!(value_true, Value::Bool(true)));
        assert!(matches!(value_false, Value::Bool(false)));
        assert!(value_true.is_bool());
        assert_eq!(value_true.as_bool(), Some(true));
    }

    #[test]
    fn test_value_int() {
        let value = Value::Int(42);
        assert!(matches!(value, Value::Int(42)));
        assert!(value.is_int());
        assert_eq!(value.as_int(), Some(42));

        let negative = Value::Int(-100);
        assert!(matches!(negative, Value::Int(-100)));
    }

    #[test]
    fn test_value_float() {
        let value = Value::Float(3.14);
        assert!(matches!(value, Value::Float(_)));
        assert!(value.is_float());

        if let Some(f) = value.as_float() {
            assert!((f - 3.14).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_value_string() {
        let value = Value::String("hello world".to_string());
        assert!(matches!(value, Value::String(_)));
        assert!(value.is_string());
        assert_eq!(value.as_str(), Some("hello world"));
    }

    #[test]
    fn test_value_bytes() {
        let bytes = vec![1, 2, 3, 4, 5];
        let value = Value::Bytes(bytes.clone());

        assert!(matches!(value, Value::Bytes(_)));
        assert!(value.is_bytes());
        assert_eq!(value.as_bytes(), Some(bytes.as_slice()));
    }

    #[test]
    fn test_value_array() {
        let array = vec![
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Bool(true),
        ];
        let value = Value::array(array.clone());

        assert!(matches!(value, Value::Array(_)));
        assert!(value.is_array());
        if let Some(arr) = value.as_array() {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::Int(1));
            assert_eq!(arr[1], Value::String("test".to_string()));
            assert_eq!(arr[2], Value::Bool(true));
        }
    }

    #[test]
    fn test_value_object() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), Value::Int(42));
        map.insert("key2".to_string(), Value::String("value".to_string()));

        let value = Value::object(map.clone());
        assert!(matches!(value, Value::Object(_)));
        assert!(value.is_object());

        if let Some(m) = value.as_object() {
            assert_eq!(m.len(), 2);
            assert_eq!(m.get("key1"), Some(&Value::Int(42)));
            assert_eq!(m.get("key2"), Some(&Value::String("value".to_string())));
        }
    }

    #[test]
    fn test_value_serialization_all_variants() {
        let test_values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.14),
            Value::String("test".to_string()),
            Value::Bytes(vec![1, 2, 3]),
            Value::array(vec![Value::Int(1), Value::String("a".to_string())]),
        ];

        for value in test_values {
            let serialized = serde_json::to_string(&value).unwrap();
            let deserialized: Value = serde_json::from_str(&serialized).unwrap();
            assert_eq!(value, deserialized);
        }
    }

    #[test]
    fn test_value_object_serialization() {
        let mut map = HashMap::new();
        map.insert("test".to_string(), Value::Int(123));
        let value = Value::object(map);

        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized: Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(value, deserialized);
    }

    // VAL-3: Different types are NEVER equal
    #[test]
    fn test_int_not_equal_float() {
        let int_val = Value::Int(1);
        let float_val = Value::Float(1.0);

        assert_ne!(int_val, float_val);
    }

    // VAL-5: IEEE-754 float equality
    #[test]
    fn test_nan_not_equal_nan() {
        let nan1 = Value::Float(f64::NAN);
        let nan2 = Value::Float(f64::NAN);

        assert_ne!(nan1, nan2);
    }

    #[test]
    fn test_negative_zero_equals_zero() {
        let neg_zero = Value::Float(-0.0);
        let zero = Value::Float(0.0);

        assert_eq!(neg_zero, zero);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(Value::Null.type_name(), "Null");
        assert_eq!(Value::Bool(true).type_name(), "Bool");
        assert_eq!(Value::Int(1).type_name(), "Int");
        assert_eq!(Value::Float(1.0).type_name(), "Float");
        assert_eq!(Value::String("".to_string()).type_name(), "String");
        assert_eq!(Value::Bytes(vec![]).type_name(), "Bytes");
        assert_eq!(Value::array(vec![]).type_name(), "Array");
        assert_eq!(Value::object(HashMap::new()).type_name(), "Object");
    }

    // ====================================================================
    // From conversions
    // ====================================================================

    #[test]
    fn test_from_i64() {
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_from_i32() {
        let v: Value = 42i32.into();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_from_f64() {
        let v: Value = 3.14f64.into();
        assert!(matches!(v, Value::Float(f) if (f - 3.14).abs() < f64::EPSILON));
    }

    #[test]
    fn test_from_f32() {
        let v: Value = 2.5f32.into();
        // Verify the actual value is preserved through f32->f64 promotion
        assert_eq!(v.as_float().unwrap(), 2.5);
    }

    #[test]
    fn test_from_bool() {
        let v: Value = true.into();
        assert_eq!(v, Value::Bool(true));
        let v: Value = false.into();
        assert_eq!(v, Value::Bool(false));
    }

    #[test]
    fn test_from_string() {
        let v: Value = String::from("hello").into();
        assert_eq!(v, Value::String("hello".to_string()));
    }

    #[test]
    fn test_from_str_ref() {
        let v: Value = "hello".into();
        assert_eq!(v, Value::String("hello".to_string()));
    }

    #[test]
    fn test_from_vec_u8() {
        let v: Value = vec![1u8, 2, 3].into();
        assert_eq!(v, Value::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn test_from_byte_slice() {
        let bytes: &[u8] = &[4, 5, 6];
        let v: Value = bytes.into();
        assert_eq!(v, Value::Bytes(vec![4, 5, 6]));
    }

    #[test]
    fn test_from_unit() {
        let v: Value = ().into();
        assert_eq!(v, Value::Null);
    }

    // ====================================================================
    // serde_json::Value interop
    // ====================================================================

    #[test]
    fn test_serde_json_value_roundtrip() {
        // Value -> serde_json::Value -> Value
        let original = Value::Int(42);
        let json: serde_json::Value = original.clone().into();
        let restored: Value = json.into();
        assert_eq!(original, restored);

        let original = Value::String("test".to_string());
        let json: serde_json::Value = original.clone().into();
        let restored: Value = json.into();
        assert_eq!(original, restored);

        let original = Value::Bool(true);
        let json: serde_json::Value = original.clone().into();
        let restored: Value = json.into();
        assert_eq!(original, restored);

        let original = Value::Null;
        let json: serde_json::Value = original.clone().into();
        let restored: Value = json.into();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_serde_json_float_nan_becomes_null() {
        // NaN cannot be represented in JSON; From<Value> for serde_json::Value maps it to Null
        let v = Value::Float(f64::NAN);
        let json: serde_json::Value = v.into();
        assert!(json.is_null());
    }

    #[test]
    fn test_serde_json_nested_conversion() {
        let json = serde_json::json!({"a": [1, 2, "three"], "b": null});
        let v: Value = json.into();
        assert!(v.is_object());
        let obj = v.as_object().unwrap();
        assert!(obj.get("a").unwrap().is_array());
        assert!(obj.get("b").unwrap().is_null());
    }

    // ====================================================================
    // as_* returns None for wrong types
    // ====================================================================

    #[test]
    fn test_as_wrong_type_returns_none() {
        let v = Value::Int(42);
        assert!(v.as_bool().is_none());
        assert!(v.as_float().is_none());
        assert!(v.as_str().is_none());
        assert!(v.as_bytes().is_none());
        assert!(v.as_array().is_none());
        assert!(v.as_object().is_none());

        let v = Value::String("hello".to_string());
        assert!(v.as_int().is_none());
        assert!(v.as_bool().is_none());
        assert!(v.as_float().is_none());
        assert!(v.as_bytes().is_none());
    }

    // ====================================================================
    // Empty container edge cases
    // ====================================================================

    #[test]
    fn test_empty_string() {
        let v = Value::String(String::new());
        assert!(v.is_string());
        assert_eq!(v.as_str(), Some(""));
    }

    #[test]
    fn test_empty_bytes() {
        let v = Value::Bytes(vec![]);
        assert!(v.is_bytes());
        assert_eq!(v.as_bytes(), Some([].as_slice()));
    }

    #[test]
    fn test_empty_array() {
        let v = Value::array(vec![]);
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_empty_object() {
        let v = Value::object(HashMap::new());
        assert!(v.is_object());
        assert_eq!(v.as_object().unwrap().len(), 0);
    }

    // ====================================================================
    // Nested structures
    // ====================================================================

    #[test]
    fn test_nested_array() {
        let inner = Value::array(vec![Value::Int(1), Value::Int(2)]);
        let outer = Value::array(vec![inner.clone(), Value::Int(3)]);
        assert!(outer.is_array());
        let arr = outer.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], inner);
    }

    #[test]
    fn test_nested_object() {
        let mut inner = HashMap::new();
        inner.insert("x".to_string(), Value::Int(1));
        let mut outer = HashMap::new();
        outer.insert("nested".to_string(), Value::object(inner));
        let v = Value::object(outer);
        assert!(v.is_object());
        let obj = v.as_object().unwrap();
        assert!(obj.get("nested").unwrap().is_object());
    }

    #[test]
    fn test_value_debug() {
        let v = Value::Int(42);
        let debug = format!("{:?}", v);
        assert!(debug.contains("42"));
    }

    // ====================================================================
    // VAL-3 extended: cross-type inequality
    // ====================================================================

    #[test]
    fn test_bytes_not_equal_string() {
        let s = Value::String("hello".to_string());
        let b = Value::Bytes(b"hello".to_vec());
        assert_ne!(s, b);
    }

    #[test]
    fn test_null_not_equal_to_other_types() {
        assert_ne!(Value::Null, Value::Bool(false));
        assert_ne!(Value::Null, Value::Int(0));
        assert_ne!(Value::Null, Value::Float(0.0));
        assert_ne!(Value::Null, Value::String(String::new()));
    }

    // ====================================================================
    // VAL-5 extended: float edge cases
    // ====================================================================

    #[test]
    fn test_float_infinity() {
        let pos_inf = Value::Float(f64::INFINITY);
        let neg_inf = Value::Float(f64::NEG_INFINITY);
        assert_eq!(pos_inf, Value::Float(f64::INFINITY));
        assert_ne!(pos_inf, neg_inf);
    }

    // ====================================================================
    // Object equality edge cases
    // ====================================================================

    #[test]
    fn test_object_equality_key_order_independent() {
        let mut m1 = HashMap::new();
        m1.insert("a".to_string(), Value::Int(1));
        m1.insert("b".to_string(), Value::Int(2));
        let mut m2 = HashMap::new();
        m2.insert("b".to_string(), Value::Int(2));
        m2.insert("a".to_string(), Value::Int(1));
        assert_eq!(Value::object(m1), Value::object(m2));
    }

    #[test]
    fn test_object_inequality_extra_key() {
        let mut m1 = HashMap::new();
        m1.insert("a".to_string(), Value::Int(1));
        let mut m2 = HashMap::new();
        m2.insert("a".to_string(), Value::Int(1));
        m2.insert("b".to_string(), Value::Int(2));
        assert_ne!(Value::object(m1), Value::object(m2));
    }

    #[test]
    fn test_deeply_nested_equality() {
        let inner = Value::array(vec![Value::object({
            let mut m = HashMap::new();
            m.insert("x".to_string(), Value::Int(1));
            m
        })]);
        let v1 = Value::array(vec![inner.clone()]);
        let v2 = Value::array(vec![inner]);
        assert_eq!(v1, v2);
    }

    // ====================================================================
    // base64 encoder correctness
    // ====================================================================

    #[test]
    fn test_base64_encode_empty() {
        assert_eq!(base64_encode_bytes(&[]), "");
    }

    #[test]
    fn test_base64_encode_known_vectors() {
        // RFC 4648 test vectors
        assert_eq!(base64_encode_bytes(b"f"), "Zg==");
        assert_eq!(base64_encode_bytes(b"fo"), "Zm8=");
        assert_eq!(base64_encode_bytes(b"foo"), "Zm9v");
        assert_eq!(base64_encode_bytes(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode_bytes(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode_bytes(b"foobar"), "Zm9vYmFy");
    }

    // ====================================================================
    // serde_json conversion edge cases
    // ====================================================================

    #[test]
    fn test_serde_json_infinity_becomes_null() {
        let v = Value::Float(f64::INFINITY);
        let json: serde_json::Value = v.into();
        assert!(json.is_null(), "Infinity should become null in JSON");
    }

    #[test]
    fn test_serde_json_neg_infinity_becomes_null() {
        let v = Value::Float(f64::NEG_INFINITY);
        let json: serde_json::Value = v.into();
        assert!(
            json.is_null(),
            "Negative infinity should become null in JSON"
        );
    }

    #[test]
    fn test_serde_json_bytes_is_lossy() {
        // Bytes -> serde_json::Value produces base64 string
        // serde_json::Value (String) -> Value produces Value::String, NOT Value::Bytes
        let original = Value::Bytes(vec![1, 2, 3]);
        let json: serde_json::Value = original.into();
        assert!(
            json.is_string(),
            "Bytes should become base64 string in JSON"
        );
        let restored: Value = json.into();
        assert!(
            restored.is_string(),
            "Converting back produces String, not Bytes (lossy)"
        );
    }

    #[test]
    fn test_serde_json_u64_max_conversion() {
        // u64::MAX cannot fit in i64, so it goes through the f64 fallback
        let json = serde_json::json!(u64::MAX);
        let v: Value = json.into();
        // Should become Float since it doesn't fit in i64
        assert!(
            v.is_float(),
            "u64::MAX should become Float since it doesn't fit in i64"
        );
    }

    #[test]
    fn test_serde_json_large_negative_int() {
        let json = serde_json::json!(i64::MIN);
        let v: Value = json.into();
        assert_eq!(v, Value::Int(i64::MIN));
    }
}
