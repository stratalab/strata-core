//! JSON path, patch, and document helpers used by engine-facing code.

pub use strata_core::primitives::json::{
    apply_patches, delete_at_path, get_at_path, get_at_path_mut, merge_patch, set_at_path,
    JsonLimitError, JsonPatch, JsonPath, JsonPathError, JsonValue, PathParseError, PathSegment,
    MAX_ARRAY_SIZE, MAX_DOCUMENT_SIZE, MAX_NESTING_DEPTH, MAX_PATH_LENGTH,
};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_limit_constants_match_legacy_core() {
        assert_eq!(MAX_PATH_LENGTH, strata_core::MAX_PATH_LENGTH);
        assert_eq!(MAX_DOCUMENT_SIZE, strata_core::MAX_DOCUMENT_SIZE);
    }

    #[test]
    fn json_helpers_behave_through_engine_surface() {
        let mut doc = JsonValue::object();
        set_at_path(
            &mut doc,
            &"user.name".parse().unwrap(),
            JsonValue::from("Alice"),
        )
        .unwrap();
        assert_eq!(
            get_at_path(&doc, &"user.name".parse().unwrap())
                .unwrap()
                .as_str(),
            Some("Alice")
        );

        let patches = vec![
            JsonPatch::set("user.age", JsonValue::from(30i64)),
            JsonPatch::delete("user.name"),
        ];
        apply_patches(&mut doc, &patches).unwrap();
        assert!(get_at_path(&doc, &"user.name".parse().unwrap()).is_none());
        assert_eq!(
            get_at_path(&doc, &"user.age".parse().unwrap())
                .unwrap()
                .as_i64(),
            Some(30)
        );

        let deleted = delete_at_path(&mut doc, &"user.age".parse().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(deleted.as_i64(), Some(30));

        let mut target: JsonValue = json!({"a": 1, "b": 2}).into();
        let patch: JsonValue = json!({"b": 3, "c": 4}).into();
        merge_patch(&mut target, &patch);
        assert_eq!(target, json!({"a": 1, "b": 3, "c": 4}).into());
    }

    #[test]
    fn json_paths_roundtrip_through_parse_and_display() {
        let path: JsonPath = "user.profile.name".parse().unwrap();
        assert_eq!(path.to_path_string(), "user.profile.name");
        let reparsed: JsonPath = path.to_path_string().parse().unwrap();
        assert_eq!(reparsed, path);
    }
}
