const MAX_DISPLAY_CANDIDATES: usize = 10;

pub(crate) fn did_you_mean(
    input: &str,
    candidates: &[String],
    max_distance: usize,
) -> Option<String> {
    let input_lower = input.to_lowercase();
    candidates
        .iter()
        .filter_map(|candidate| {
            let distance = strsim::levenshtein(&input_lower, &candidate.to_lowercase());
            (distance <= max_distance && distance > 0).then_some((candidate.clone(), distance))
        })
        .min_by_key(|(_, distance)| *distance)
        .map(|(candidate, _)| candidate)
}

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
            .map(String::as_str)
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
    fn exact_matches_are_not_suggested() {
        let candidates = vec!["feature".to_string()];
        assert_eq!(did_you_mean("feature", &candidates, 2), None);
    }

    #[test]
    fn hint_truncates_long_candidate_lists() {
        let candidates: Vec<String> = (0..25).map(|i| format!("branch-{i}")).collect();
        let hint = format_hint("branches", &candidates, "missing", 2)
            .expect("non-empty candidates should produce a hint");

        assert!(hint.contains("and 15 more"), "hint = {hint}");
        assert!(hint.contains("branch-0"), "hint = {hint}");
        assert!(!hint.contains("branch-10"), "hint = {hint}");
    }
}
