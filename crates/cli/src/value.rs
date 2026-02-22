//! String → Value parsing rules.
//!
//! User input is parsed into `Value` using auto-detect logic:
//! 1. JSON structures (`{`, `[`, `"`) → parse as JSON
//! 2. `null` → Value::Null
//! 3. `true` / `false` → Value::Bool
//! 4. Integer pattern → Value::Int
//! 5. Float pattern → Value::Float
//! 6. Everything else → Value::String

use strata_executor::Value;

/// Auto-detect value type from a user-supplied string.
///
/// Rules applied in order:
/// 1. If starts with `{`, `[`, or `"` → parse as JSON, convert via `From<serde_json::Value>`
/// 2. `null` → `Value::Null`
/// 3. `true` / `false` → `Value::Bool`
/// 4. Matches `^-?[0-9]+$` and fits i64 → `Value::Int`
/// 5. Matches float pattern → `Value::Float`
/// 6. Everything else → `Value::String`
pub fn parse_value(s: &str) -> Value {
    // Rule 1: JSON structures
    if s.starts_with('{') || s.starts_with('[') || s.starts_with('"') {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
            return Value::from(json);
        }
        // If JSON parse fails, fall through to string
    }

    // Rule 2: null
    if s == "null" {
        return Value::Null;
    }

    // Rule 3: booleans
    if s == "true" {
        return Value::Bool(true);
    }
    if s == "false" {
        return Value::Bool(false);
    }

    // Rule 4: integers
    if is_integer(s) {
        if let Ok(i) = s.parse::<i64>() {
            return Value::Int(i);
        }
    }

    // Rule 5: floats
    if is_float(s) {
        if let Ok(f) = s.parse::<f64>() {
            return Value::Float(f);
        }
    }

    // Rule 6: everything else is a string
    Value::String(s.to_string())
}

/// Strict JSON parsing — input must be valid JSON.
pub fn parse_json_value(s: &str) -> Result<Value, String> {
    let json: serde_json::Value =
        serde_json::from_str(s).map_err(|e| format!("Invalid JSON: {}", e))?;
    Ok(Value::from(json))
}

/// Parse a vector literal like `[1.0, 2.0, 3.0]`.
pub fn parse_vector(s: &str) -> Result<Vec<f32>, String> {
    let json: serde_json::Value =
        serde_json::from_str(s).map_err(|e| format!("Invalid vector literal: {}", e))?;
    match json {
        serde_json::Value::Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v.as_f64() {
                    Some(f) => result.push(f as f32),
                    None => return Err(format!("Element {} is not a number", i)),
                }
            }
            Ok(result)
        }
        _ => Err("Expected a JSON array of numbers".to_string()),
    }
}

fn is_integer(s: &str) -> bool {
    let s = if let Some(rest) = s.strip_prefix('-') {
        rest
    } else {
        s
    };
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn is_float(s: &str) -> bool {
    let s = if let Some(rest) = s.strip_prefix('-') {
        rest
    } else {
        s
    };
    if s.is_empty() {
        return false;
    }
    // Must contain a dot or exponent
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        return false;
    }
    // All characters must be digits, dot, e, E, +, -
    s.bytes().all(|b| {
        b.is_ascii_digit() || b == b'.' || b == b'e' || b == b'E' || b == b'+' || b == b'-'
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        assert_eq!(parse_value("null"), Value::Null);
    }

    #[test]
    fn test_parse_booleans() {
        assert_eq!(parse_value("true"), Value::Bool(true));
        assert_eq!(parse_value("false"), Value::Bool(false));
    }

    #[test]
    fn test_parse_integers() {
        assert_eq!(parse_value("42"), Value::Int(42));
        assert_eq!(parse_value("-1"), Value::Int(-1));
        assert_eq!(parse_value("0"), Value::Int(0));
    }

    #[test]
    fn test_parse_floats() {
        assert_eq!(parse_value("3.125"), Value::Float(3.125));
        assert_eq!(parse_value("-0.5"), Value::Float(-0.5));
        assert_eq!(parse_value("1.0e10"), Value::Float(1.0e10));
    }

    #[test]
    fn test_parse_strings() {
        assert_eq!(parse_value("hello"), Value::String("hello".into()));
        assert_eq!(parse_value("foo bar"), Value::String("foo bar".into()));
    }

    #[test]
    fn test_parse_json_object() {
        match parse_value(r#"{"name":"Alice"}"#) {
            Value::Object(map) => {
                assert_eq!(map.get("name"), Some(&Value::String("Alice".into())));
            }
            other => panic!("Expected Object, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_json_array() {
        match parse_value("[1,2,3]") {
            Value::Array(arr) => assert_eq!(arr.len(), 3),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_vector() {
        let v = parse_vector("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_json_value_strict() {
        assert!(parse_json_value("not json").is_err());
        assert!(parse_json_value(r#"{"a":1}"#).is_ok());
    }
}
