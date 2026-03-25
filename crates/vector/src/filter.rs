//! Metadata filtering for vector search
//!
//! Re-exports canonical types from strata-core.
//! Supports only equality filtering on top-level scalar fields.
//! Complex filters (ranges, nested paths, arrays) are deferred to future versions.

// Re-export canonical filter types from core
pub use strata_core::primitives::{FilterCondition, FilterOp, JsonScalar, MetadataFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serde_json::Value as JsonValue;

    #[test]
    fn test_empty_filter_matches_all() {
        let filter = MetadataFilter::new();
        assert!(filter.matches(&None));
        assert!(filter.matches(&Some(json!({"foo": "bar"}))));
        assert!(filter.is_empty());
        assert_eq!(filter.len(), 0);
    }

    #[test]
    fn test_filter_matches_exact() {
        let filter = MetadataFilter::new()
            .eq("category", "document")
            .eq("year", 2024);

        // Matching metadata
        let meta = json!({
            "category": "document",
            "year": 2024,
            "extra": "ignored"
        });
        assert!(filter.matches(&Some(meta)));
        assert!(!filter.is_empty());
        assert_eq!(filter.len(), 2);
    }

    #[test]
    fn test_filter_missing_field() {
        let filter = MetadataFilter::new()
            .eq("category", "document")
            .eq("year", 2024);

        let meta = json!({ "category": "document" });
        assert!(!filter.matches(&Some(meta)));
    }

    #[test]
    fn test_filter_wrong_value() {
        let filter = MetadataFilter::new().eq("category", "document");

        let meta = json!({ "category": "image" });
        assert!(!filter.matches(&Some(meta)));
    }

    #[test]
    fn test_filter_none_metadata() {
        let filter = MetadataFilter::new().eq("category", "document");
        assert!(!filter.matches(&None));
    }

    #[test]
    fn test_filter_non_object_metadata() {
        let filter = MetadataFilter::new().eq("category", "document");
        assert!(!filter.matches(&Some(json!("not an object"))));
        assert!(!filter.matches(&Some(json!([1, 2, 3]))));
    }

    #[test]
    fn test_filter_bool_value() {
        let filter = MetadataFilter::new().eq("active", true);

        assert!(filter.matches(&Some(json!({ "active": true }))));
        assert!(!filter.matches(&Some(json!({ "active": false }))));
    }

    #[test]
    fn test_filter_null_value() {
        let filter = MetadataFilter::new().eq("deleted", JsonScalar::Null);

        assert!(filter.matches(&Some(json!({ "deleted": null }))));
        assert!(!filter.matches(&Some(json!({ "deleted": false }))));
    }

    #[test]
    fn test_filter_number_value() {
        let filter = MetadataFilter::new().eq("count", 42);

        assert!(filter.matches(&Some(json!({ "count": 42 }))));
        assert!(!filter.matches(&Some(json!({ "count": 43 }))));
    }

    #[test]
    fn test_filter_float_value() {
        let filter = MetadataFilter::new().eq("score", 0.95f64);

        assert!(filter.matches(&Some(json!({ "score": 0.95 }))));
        assert!(!filter.matches(&Some(json!({ "score": 0.96 }))));
    }

    #[test]
    fn test_json_scalar_equality() {
        assert_eq!(JsonScalar::Null, JsonScalar::Null);
        assert_eq!(JsonScalar::Bool(true), JsonScalar::Bool(true));
        assert_ne!(JsonScalar::Bool(true), JsonScalar::Bool(false));
        assert_eq!(JsonScalar::Number(42.0), JsonScalar::Number(42.0));
        assert_eq!(
            JsonScalar::String("test".to_string()),
            JsonScalar::String("test".to_string())
        );
    }

    #[test]
    fn test_json_scalar_from_conversions() {
        let _: JsonScalar = true.into();
        let _: JsonScalar = 42i32.into();
        let _: JsonScalar = 42i64.into();
        let _: JsonScalar = 42.0f64.into();
        let _: JsonScalar = 42.0f32.into();
        let _: JsonScalar = "test".into();
        let _: JsonScalar = String::from("test").into();
    }

    #[test]
    fn test_json_scalar_matches_json() {
        assert!(JsonScalar::Null.matches_json(&JsonValue::Null));
        assert!(JsonScalar::Bool(true).matches_json(&json!(true)));
        assert!(JsonScalar::Number(42.0).matches_json(&json!(42)));
        assert!(JsonScalar::String("test".to_string()).matches_json(&json!("test")));

        // Type mismatches
        assert!(!JsonScalar::Bool(true).matches_json(&json!(1)));
        assert!(!JsonScalar::Number(42.0).matches_json(&json!("42")));
        assert!(!JsonScalar::String("42".to_string()).matches_json(&json!(42)));
    }

    #[test]
    fn test_filter_chaining() {
        let filter = MetadataFilter::new().eq("a", "1").eq("b", "2").eq("c", "3");

        assert_eq!(filter.len(), 3);

        let meta = json!({
            "a": "1",
            "b": "2",
            "c": "3"
        });
        assert!(filter.matches(&Some(meta)));

        let partial = json!({
            "a": "1",
            "b": "2"
        });
        assert!(!filter.matches(&Some(partial)));
    }
}
