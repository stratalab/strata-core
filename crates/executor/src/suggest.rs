//! Fuzzy suggestion utilities for actionable error messages.

/// Maximum number of candidates to display in hint messages.
const MAX_DISPLAY_CANDIDATES: usize = 10;

/// Find the closest match to `input` among `candidates` using Levenshtein distance.
///
/// Returns `None` if no candidate is within `max_distance` edits.
pub(crate) fn did_you_mean(
    input: &str,
    candidates: &[String],
    max_distance: usize,
) -> Option<String> {
    let input_lower = input.to_lowercase();
    candidates
        .iter()
        .filter_map(|c| {
            let d = strsim::levenshtein(&input_lower, &c.to_lowercase());
            (d <= max_distance && d > 0).then_some((c.clone(), d))
        })
        .min_by_key(|(_, d)| *d)
        .map(|(s, _)| s)
}

/// Build a hint string listing available options and optionally suggesting the closest match.
///
/// Example output:
/// - `"Available branches: default, feature, staging. Did you mean 'feature'?"`
/// - `"Available branches: default, feature."` (no close match)
/// - `None` (empty candidates)
///
/// When there are more than 10 candidates, the list is truncated with "and N more".
pub(crate) fn format_hint(
    entity_plural: &str,
    candidates: &[String],
    input: &str,
    max_distance: usize,
) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }
    let list = if candidates.len() <= MAX_DISPLAY_CANDIDATES {
        candidates.join(", ")
    } else {
        let shown: Vec<&str> = candidates[..MAX_DISPLAY_CANDIDATES]
            .iter()
            .map(|s| s.as_str())
            .collect();
        format!(
            "{} (and {} more)",
            shown.join(", "),
            candidates.len() - MAX_DISPLAY_CANDIDATES
        )
    };
    match did_you_mean(input, candidates, max_distance) {
        Some(suggestion) => Some(format!(
            "Available {}: {}. Did you mean '{}'?",
            entity_plural, list, suggestion
        )),
        None => Some(format!("Available {}: {}.", entity_plural, list)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_match_found() {
        let candidates = vec!["feature".into(), "staging".into(), "default".into()];
        assert_eq!(
            did_you_mean("featuer", &candidates, 2),
            Some("feature".into())
        );
    }

    #[test]
    fn no_match_too_far() {
        let candidates = vec!["feature".into(), "staging".into()];
        assert_eq!(did_you_mean("zzzzzzz", &candidates, 2), None);
    }

    #[test]
    fn case_insensitive() {
        let candidates = vec!["Feature".into()];
        assert_eq!(
            did_you_mean("FEATUER", &candidates, 2),
            Some("Feature".into())
        );
    }

    #[test]
    fn empty_candidates() {
        let candidates: Vec<String> = vec![];
        assert_eq!(did_you_mean("anything", &candidates, 2), None);
    }

    #[test]
    fn format_hint_with_suggestion() {
        let candidates = vec!["default".into(), "feature".into(), "staging".into()];
        let hint = format_hint("branches", &candidates, "featuer", 2).unwrap();
        assert!(hint.contains("Did you mean 'feature'?"));
        assert!(hint.contains("Available branches:"));
    }

    #[test]
    fn format_hint_without_suggestion() {
        let candidates = vec!["default".into(), "feature".into()];
        let hint = format_hint("branches", &candidates, "zzzzzzz", 2).unwrap();
        assert!(!hint.contains("Did you mean"));
        assert!(hint.contains("Available branches:"));
    }

    #[test]
    fn format_hint_empty_candidates() {
        let candidates: Vec<String> = vec![];
        assert_eq!(format_hint("branches", &candidates, "anything", 2), None);
    }

    #[test]
    fn exact_match_not_suggested() {
        let candidates = vec!["feature".into()];
        // Exact match has distance 0, should not be suggested (d > 0 filter)
        assert_eq!(did_you_mean("feature", &candidates, 2), None);
    }

    #[test]
    fn format_hint_caps_long_candidate_list() {
        let candidates: Vec<String> = (0..25).map(|i| format!("branch-{}", i)).collect();
        let hint = format_hint("branches", &candidates, "zzzzzzz", 2).unwrap();
        // Should show first 10 and "and 15 more"
        assert!(hint.contains("and 15 more"), "hint = {}", hint);
        assert!(hint.contains("branch-0"));
        assert!(hint.contains("branch-9"));
        assert!(
            !hint.contains("branch-10"),
            "should not show 11th candidate"
        );
    }

    #[test]
    fn format_hint_cap_still_shows_suggestion() {
        let mut candidates: Vec<String> = (0..20).map(|i| format!("branch-{}", i)).collect();
        candidates.push("feature".into()); // candidate beyond the display cap
        let hint = format_hint("branches", &candidates, "featuer", 2).unwrap();
        // The suggestion should still work even if "feature" is past the cap
        assert!(hint.contains("Did you mean 'feature'?"), "hint = {}", hint);
        assert!(hint.contains("and 11 more"));
    }

    #[test]
    fn unicode_case_insensitive() {
        let candidates = vec!["Ärger".into()];
        assert_eq!(
            did_you_mean("ärger", &candidates, 2),
            None, // exact match after lowercasing → distance 0
        );
        // One edit away
        assert_eq!(did_you_mean("ärge", &candidates, 2), Some("Ärger".into()),);
    }

    #[test]
    fn serde_backward_compat_error_without_hint() {
        // Simulate deserializing an error JSON from an older version (no hint field)
        let json = r#"{"BranchNotFound":{"branch":"test"}}"#;
        let err: crate::Error = serde_json::from_str(json).unwrap();
        match err {
            crate::Error::BranchNotFound { branch, hint } => {
                assert_eq!(branch, "test");
                assert_eq!(hint, None);
            }
            other => panic!("Expected BranchNotFound, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_error_with_hint() {
        let err = crate::Error::BranchNotFound {
            branch: "featuer".into(),
            hint: Some("Did you mean 'feature'?".into()),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("hint"), "hint should be serialized: {}", json);
        let roundtrip: crate::Error = serde_json::from_str(&json).unwrap();
        assert_eq!(err, roundtrip);
    }

    #[test]
    fn serde_hint_none_omitted() {
        let err = crate::Error::BranchNotFound {
            branch: "test".into(),
            hint: None,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(
            !json.contains("hint"),
            "hint: None should be omitted: {}",
            json
        );
    }

    #[test]
    fn error_display_with_hint() {
        let err = crate::Error::BranchNotFound {
            branch: "featuer".into(),
            hint: Some("Did you mean 'feature'?".into()),
        };
        let msg = err.to_string();
        assert!(msg.contains("featuer"), "msg = {}", msg);
        assert!(msg.contains("Did you mean 'feature'?"), "msg = {}", msg);
    }

    #[test]
    fn error_display_without_hint() {
        let err = crate::Error::BranchNotFound {
            branch: "test".into(),
            hint: None,
        };
        let msg = err.to_string();
        assert_eq!(msg, "branch not found: test");
    }
}
