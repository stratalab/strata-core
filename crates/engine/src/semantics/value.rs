//! Engine-owned runtime and search-facing helpers over foundational values.

use strata_core::Value;

/// Extract indexable text from a value for keyword search.
///
/// String values index directly. Numeric and structured values are serialized
/// through JSON. Nulls, booleans, and raw bytes do not contribute searchable
/// text.
pub fn extractable_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Null | Value::Bool(_) | Value::Bytes(_) => None,
        other => serde_json::to_string(&serde_json::Value::from(other.clone())).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::extractable_text;
    use strata_core::Value;

    #[test]
    fn extractable_text_matches_engine_contract() {
        assert_eq!(
            extractable_text(&Value::String("hello".to_string())),
            Some("hello".to_string())
        );
        assert_eq!(extractable_text(&Value::Null), None);
        assert_eq!(extractable_text(&Value::Bool(true)), None);
        assert_eq!(extractable_text(&Value::Bytes(vec![1, 2, 3])), None);
        assert_eq!(extractable_text(&Value::Int(42)), Some("42".to_string()));

        let composite = Value::array(vec![Value::Int(1), Value::String("a".into())]);
        assert_eq!(extractable_text(&composite), Some("[1,\"a\"]".to_string()));
    }
}
