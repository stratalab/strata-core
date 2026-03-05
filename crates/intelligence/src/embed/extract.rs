//! Value → embeddable text extraction.

use std::cell::Cell;

use strata_core::Value;

/// Maximum text length sent to the tokenizer (≈256 tokens).
const MAX_TEXT_LEN: usize = 1024;

/// Maximum recursion depth for nested Array/Object values.
const MAX_DEPTH: usize = 16;

/// Extract embeddable text from a Value, with error reporting for depth-limit
/// truncation.
///
/// Returns:
/// - `Ok(Some(text))` for successful extraction with text
/// - `Ok(None)` for values with no extractable text (Null, Bytes, empty string, etc.)
/// - `Err("depth limit exceeded: ...")` when recursion was truncated due to MAX_DEPTH
///
/// When truncation occurs, any partial text from shallower branches is discarded.
/// Use [`extract_text`] instead if you want best-effort partial results.
pub fn extract_text_checked(value: &Value) -> Result<Option<String>, String> {
    let truncated = Cell::new(false);
    let text = extract_text_inner(value, 0, &truncated);

    let text = match text {
        Some(t) if t.len() > MAX_TEXT_LEN => {
            let mut end = MAX_TEXT_LEN;
            while end > 0 && !t.is_char_boundary(end) {
                end -= 1;
            }
            Some(t[..end].to_string())
        }
        other => other,
    };

    if truncated.get() {
        Err(format!("depth limit exceeded (max depth {})", MAX_DEPTH))
    } else {
        Ok(text)
    }
}

/// Extract embeddable text from a Value.
///
/// Returns `None` for values that are not meaningful to embed (Null, Bytes).
/// Nested structures are traversed up to `MAX_DEPTH` levels to prevent
/// stack overflow on pathologically deep documents.
pub fn extract_text(value: &Value) -> Option<String> {
    // Best-effort: ignore depth-limit truncation, return whatever text was found.
    let truncated = Cell::new(false);
    let text = extract_text_inner(value, 0, &truncated);

    match text {
        Some(t) if t.len() > MAX_TEXT_LEN => {
            let mut end = MAX_TEXT_LEN;
            while end > 0 && !t.is_char_boundary(end) {
                end -= 1;
            }
            Some(t[..end].to_string())
        }
        other => other,
    }
}

fn extract_text_inner(value: &Value, depth: usize, truncated: &Cell<bool>) -> Option<String> {
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
                truncated.set(true);
                return None;
            }
            let parts: Vec<String> = arr
                .iter()
                .filter_map(|v| extract_text_inner(v, depth + 1, truncated))
                .collect();
            if parts.is_empty() {
                return None;
            }
            Some(parts.join(" "))
        }
        Value::Object(map) => {
            if depth >= MAX_DEPTH {
                truncated.set(true);
                return None;
            }
            let mut parts: Vec<String> = Vec::new();
            // Sort keys for deterministic output
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(val_text) = extract_text_inner(&map[key], depth + 1, truncated) {
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
        let arr = Value::array(vec![
            Value::String("hello".into()),
            Value::Int(42),
            Value::Null,
        ]);
        assert_eq!(extract_text(&arr), Some("hello 42".into()));
    }

    #[test]
    fn test_object() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".into()));
        map.insert("age".to_string(), Value::Int(30));
        let obj = Value::object(map);
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
        assert_eq!(extract_text(&Value::array(vec![])), None);
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
            extract_text(&Value::array(vec![Value::Null, Value::Null])),
            None
        );
    }

    #[test]
    fn test_depth_limit() {
        // Build a deeply nested structure exceeding MAX_DEPTH
        let mut value = Value::String("deep".into());
        for _ in 0..20 {
            value = Value::array(vec![value]);
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
            value = Value::array(vec![value]);
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
        let arr = Value::array(vec![Value::object(m1), Value::object(m2)]);
        let text = extract_text(&arr).unwrap();
        assert!(text.contains("name: Alice"));
        assert!(text.contains("name: Bob"));
    }

    #[test]
    fn test_object_all_null_values() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Null);
        map.insert("b".to_string(), Value::Null);
        assert_eq!(extract_text(&Value::object(map)), None);
    }

    #[test]
    fn test_empty_object() {
        let map = HashMap::new();
        assert_eq!(extract_text(&Value::object(map)), None);
    }

    #[test]
    fn test_object_keys_sorted() {
        let mut map = HashMap::new();
        map.insert("z".to_string(), Value::String("last".into()));
        map.insert("a".to_string(), Value::String("first".into()));
        map.insert("m".to_string(), Value::String("middle".into()));
        let text = extract_text(&Value::object(map)).unwrap();
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
        let arr = Value::array(vec![
            Value::Int(1),
            Value::String("two".into()),
            Value::object(inner),
        ]);
        let mut outer = HashMap::new();
        outer.insert("items".to_string(), arr);
        let text = extract_text(&Value::object(outer)).unwrap();
        assert!(text.contains("items:"));
        assert!(text.contains("1"));
        assert!(text.contains("two"));
        assert!(text.contains("nested: true"));
    }

    // -----------------------------------------------------------------------
    // extract_text_checked tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_text_checked_depth_limit_returns_err() {
        // Build a deeply nested structure exceeding MAX_DEPTH (20 levels)
        let mut value = Value::String("deep".into());
        for _ in 0..20 {
            value = Value::array(vec![value]);
        }
        let result = extract_text_checked(&value);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("depth limit"),
            "error should mention depth limit, got: {}",
            err
        );
    }

    #[test]
    fn test_extract_text_checked_normal_returns_ok_some() {
        let result = extract_text_checked(&Value::String("text".into()));
        assert_eq!(result, Ok(Some("text".to_string())));
    }

    #[test]
    fn test_extract_text_checked_null_returns_ok_none() {
        let result = extract_text_checked(&Value::Null);
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_shallow_text_preserved_when_deep_branch_truncated() {
        // Array with a shallow string and a deeply nested branch.
        // extract_text should return the shallow text (best-effort),
        // while extract_text_checked should return Err (truncation detected).
        let mut deep = Value::String("unreachable".into());
        for _ in 0..20 {
            deep = Value::array(vec![deep]);
        }
        let mixed = Value::array(vec![Value::String("shallow".into()), deep]);

        // extract_text preserves partial results (backward compat)
        let text = extract_text(&mixed);
        assert_eq!(
            text,
            Some("shallow".into()),
            "extract_text should return shallow text even when a deep branch is truncated"
        );

        // extract_text_checked signals truncation
        let checked = extract_text_checked(&mixed);
        assert!(
            checked.is_err(),
            "extract_text_checked should return Err when any branch hit depth limit"
        );
    }

    #[test]
    fn test_checked_at_exact_limit_is_ok() {
        // 15 levels of wrapping → depth 15 is a String, within MAX_DEPTH=16.
        let mut value = Value::String("within".into());
        for _ in 0..15 {
            value = Value::array(vec![value]);
        }
        assert_eq!(extract_text_checked(&value), Ok(Some("within".into())),);
    }

    #[test]
    fn test_checked_truncation_in_object_value() {
        // Object where one value is too deep, another is shallow.
        let mut deep = Value::String("hidden".into());
        for _ in 0..20 {
            deep = Value::array(vec![deep]);
        }
        let mut map = HashMap::new();
        map.insert("shallow".to_string(), Value::String("visible".into()));
        map.insert("deep".to_string(), deep);
        let obj = Value::object(map);

        // extract_text returns partial text
        let text = extract_text(&obj).unwrap();
        assert!(
            text.contains("shallow: visible"),
            "shallow key should be present: {}",
            text
        );

        // extract_text_checked signals truncation
        assert!(extract_text_checked(&obj).is_err());
    }
}
