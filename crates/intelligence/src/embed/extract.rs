//! Value → embeddable text extraction.

use strata_core::Value;

/// Maximum text length sent to the tokenizer (≈256 tokens).
const MAX_TEXT_LEN: usize = 1024;

/// Maximum recursion depth for nested Array/Object values.
const MAX_DEPTH: usize = 16;

/// Extract embeddable text from a Value.
///
/// Returns `None` for values that are not meaningful to embed (Null, Bytes).
/// Nested structures are traversed up to `MAX_DEPTH` levels to prevent
/// stack overflow on pathologically deep documents.
pub fn extract_text(value: &Value) -> Option<String> {
    let text = extract_text_inner(value, 0)?;

    if text.len() > MAX_TEXT_LEN {
        // Find the last char boundary at or before MAX_TEXT_LEN to avoid
        // panicking on multi-byte UTF-8 characters.
        let mut end = MAX_TEXT_LEN;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        Some(text[..end].to_string())
    } else {
        Some(text)
    }
}

fn extract_text_inner(value: &Value, depth: usize) -> Option<String> {
    match value {
        Value::String(s) => {
            if s.is_empty() {
                return None;
            }
            Some(s.clone())
        }
        Value::Int(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        Value::Bytes(_) => None,
        Value::Array(arr) => {
            if depth >= MAX_DEPTH {
                return None;
            }
            let parts: Vec<String> = arr
                .iter()
                .filter_map(|v| extract_text_inner(v, depth + 1))
                .collect();
            if parts.is_empty() {
                return None;
            }
            Some(parts.join(" "))
        }
        Value::Object(map) => {
            if depth >= MAX_DEPTH {
                return None;
            }
            let mut parts: Vec<String> = Vec::new();
            // Sort keys for deterministic output
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(val_text) = extract_text_inner(&map[key], depth + 1) {
                    parts.push(format!("{}: {}", key, val_text));
                }
            }
            if parts.is_empty() {
                return None;
            }
            Some(parts.join(" "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_string() {
        assert_eq!(
            extract_text(&Value::String("hello".into())),
            Some("hello".into())
        );
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(extract_text(&Value::String("".into())), None);
    }

    #[test]
    fn test_int() {
        assert_eq!(extract_text(&Value::Int(42)), Some("42".into()));
    }

    #[test]
    fn test_float() {
        assert_eq!(extract_text(&Value::Float(3.14)), Some("3.14".into()));
    }

    #[test]
    fn test_bool() {
        assert_eq!(extract_text(&Value::Bool(true)), Some("true".into()));
    }

    #[test]
    fn test_null() {
        assert_eq!(extract_text(&Value::Null), None);
    }

    #[test]
    fn test_bytes() {
        assert_eq!(extract_text(&Value::Bytes(vec![1, 2, 3])), None);
    }

    #[test]
    fn test_array() {
        let arr = Value::Array(Box::new(vec![
            Value::String("hello".into()),
            Value::Int(42),
            Value::Null,
        ]));
        assert_eq!(extract_text(&arr), Some("hello 42".into()));
    }

    #[test]
    fn test_object() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".into()));
        map.insert("age".to_string(), Value::Int(30));
        let obj = Value::Object(Box::new(map));
        let text = extract_text(&obj).unwrap();
        assert!(text.contains("age: 30"));
        assert!(text.contains("name: Alice"));
    }

    #[test]
    fn test_truncation() {
        let long_str = Value::String("a".repeat(2000));
        let text = extract_text(&long_str).unwrap();
        assert_eq!(text.len(), MAX_TEXT_LEN);
    }

    #[test]
    fn test_empty_array() {
        assert_eq!(extract_text(&Value::Array(Box::new(vec![]))), None);
    }

    #[test]
    fn test_truncation_multibyte_safe() {
        // Each '😀' is 4 bytes. Fill past MAX_TEXT_LEN to ensure we don't panic
        // on a multi-byte char boundary.
        let emoji_str = "😀".repeat(300); // 300 * 4 = 1200 bytes > 1024
        let text = extract_text(&Value::String(emoji_str)).unwrap();
        assert!(text.len() <= MAX_TEXT_LEN);
        // Must be valid UTF-8 (no panic, no partial chars)
        assert!(text.is_char_boundary(text.len()));
    }

    #[test]
    fn test_array_all_null() {
        assert_eq!(
            extract_text(&Value::Array(Box::new(vec![Value::Null, Value::Null]))),
            None
        );
    }

    #[test]
    fn test_depth_limit() {
        // Build a deeply nested structure exceeding MAX_DEPTH
        let mut value = Value::String("deep".into());
        for _ in 0..20 {
            value = Value::Array(Box::new(vec![value]));
        }
        // The leaf "deep" is at depth 20, which exceeds MAX_DEPTH (16).
        // extract_text should return None because the recursion stops before reaching it.
        assert_eq!(extract_text(&value), None);
    }

    #[test]
    fn test_depth_at_limit() {
        // Build a nested structure exactly at MAX_DEPTH (16 levels of nesting)
        let mut value = Value::String("found".into());
        for _ in 0..15 {
            value = Value::Array(Box::new(vec![value]));
        }
        // 15 levels of Array wrapping: depths 0..14 are Array, depth 15 is String.
        // All within MAX_DEPTH (16), so the text should be extractable.
        assert_eq!(extract_text(&value), Some("found".into()));
    }

    #[test]
    fn test_nested_object_in_array() {
        let mut m1 = HashMap::new();
        m1.insert("name".to_string(), Value::String("Alice".into()));
        let mut m2 = HashMap::new();
        m2.insert("name".to_string(), Value::String("Bob".into()));
        let arr = Value::Array(Box::new(vec![
            Value::Object(Box::new(m1)),
            Value::Object(Box::new(m2)),
        ]));
        let text = extract_text(&arr).unwrap();
        assert!(text.contains("name: Alice"));
        assert!(text.contains("name: Bob"));
    }

    #[test]
    fn test_object_all_null_values() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Null);
        map.insert("b".to_string(), Value::Null);
        assert_eq!(extract_text(&Value::Object(Box::new(map))), None);
    }

    #[test]
    fn test_empty_object() {
        let map = HashMap::new();
        assert_eq!(extract_text(&Value::Object(Box::new(map))), None);
    }

    #[test]
    fn test_object_keys_sorted() {
        let mut map = HashMap::new();
        map.insert("z".to_string(), Value::String("last".into()));
        map.insert("a".to_string(), Value::String("first".into()));
        map.insert("m".to_string(), Value::String("middle".into()));
        let text = extract_text(&Value::Object(Box::new(map))).unwrap();
        let a_pos = text.find("a:").unwrap();
        let m_pos = text.find("m:").unwrap();
        let z_pos = text.find("z:").unwrap();
        assert!(a_pos < m_pos, "a should come before m");
        assert!(m_pos < z_pos, "m should come before z");
    }

    #[test]
    fn test_mixed_depth_objects_and_arrays() {
        let mut inner = HashMap::new();
        inner.insert("nested".to_string(), Value::Bool(true));
        let arr = Value::Array(Box::new(vec![
            Value::Int(1),
            Value::String("two".into()),
            Value::Object(Box::new(inner)),
        ]));
        let mut outer = HashMap::new();
        outer.insert("items".to_string(), arr);
        let text = extract_text(&Value::Object(Box::new(outer))).unwrap();
        assert!(text.contains("items:"));
        assert!(text.contains("1"));
        assert!(text.contains("two"));
        assert!(text.contains("nested: true"));
    }
}
