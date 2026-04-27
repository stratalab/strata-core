//! Value helpers shared by search and display code.

pub use strata_core_foundation::value::*;

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

/// Extension trait for deriving indexable text from a value.
pub trait ValueTextExt {
    /// Extract indexable text from this value for keyword search.
    fn extractable_text(&self) -> Option<String>;
}

impl ValueTextExt for Value {
    fn extractable_text(&self) -> Option<String> {
        extractable_text(self)
    }
}

#[doc(hidden)]
pub use ValueTextExt as LegacyValueExt;

#[cfg(test)]
mod tests {
    use super::{extractable_text, Value, ValueTextExt};

    #[test]
    fn extractable_text_matches_surface_contract() {
        assert_eq!(
            Value::String("hello".to_string()).extractable_text(),
            Some("hello".to_string())
        );
        assert_eq!(Value::Null.extractable_text(), None);
        assert_eq!(Value::Bool(true).extractable_text(), None);
        assert_eq!(Value::Bytes(vec![1, 2, 3]).extractable_text(), None);
        let int_value = Value::Int(42);
        assert_eq!(int_value.extractable_text(), Some("42".to_string()));
    }

    #[test]
    fn free_function_matches_extension_trait() {
        let value = Value::array(vec![Value::Int(1), Value::String("a".into())]);
        assert_eq!(extractable_text(&value), value.extractable_text());
    }
}
