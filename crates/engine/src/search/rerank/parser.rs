//! Response parser for rerank score output.

use super::RerankScore;

/// Parse model output into rerank scores.
///
/// Expects lines like `1: 8` or `2: 5.5`. Maps 1-based line numbers back to
/// the original snippet indices. Scores are clamped to `0..=10` and normalized
/// to `[0.0, 1.0]`.
pub fn parse_rerank_response(text: &str, snippets: &[(usize, &str)]) -> Vec<RerankScore> {
    let mut scores = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some((num_part, score_part)) = line.split_once(':') {
            let num_part = num_part.trim();
            let score_part = score_part.trim();

            if let (Ok(line_num), Ok(raw_score)) =
                (num_part.parse::<usize>(), score_part.parse::<f32>())
            {
                if line_num >= 1 && line_num <= snippets.len() {
                    let (orig_index, _) = snippets[line_num - 1];
                    let clamped = raw_score.clamp(0.0, 10.0);
                    scores.push(RerankScore {
                        index: orig_index,
                        relevance_score: clamped / 10.0,
                    });
                }
            }
        }
    }

    scores
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rerank_response_basic() {
        let snippets = vec![(0, "doc a"), (1, "doc b"), (2, "doc c")];
        let text = "1: 8\n2: 5\n3: 3\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 3);
        assert_eq!(scores[0].index, 0);
        assert!((scores[0].relevance_score - 0.8).abs() < f32::EPSILON);
        assert_eq!(scores[1].index, 1);
        assert!((scores[1].relevance_score - 0.5).abs() < f32::EPSILON);
        assert_eq!(scores[2].index, 2);
        assert!((scores[2].relevance_score - 0.3).abs() < f32::EPSILON);
    }

    #[test]
    fn parse_rerank_response_with_decimals() {
        let snippets = vec![(5, "doc a"), (10, "doc b")];
        let text = "1: 7.5\n2: 3.2\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 2);
        assert_eq!(scores[0].index, 5);
        assert!((scores[0].relevance_score - 0.75).abs() < f32::EPSILON);
        assert_eq!(scores[1].index, 10);
        assert!((scores[1].relevance_score - 0.32).abs() < 0.01);
    }

    #[test]
    fn parse_rerank_response_clamps_scores() {
        let snippets = vec![(0, "doc a")];
        let text = "1: 15\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 1);
        assert!((scores[0].relevance_score - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn parse_rerank_response_ignores_invalid_lines() {
        let snippets = vec![(0, "doc a"), (1, "doc b")];
        let text = "1: 8\nsome garbage\n2: five\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].index, 0);
    }

    #[test]
    fn parse_rerank_response_empty() {
        let snippets = vec![(0, "doc a")];
        let text = "";
        let scores = parse_rerank_response(text, &snippets);
        assert!(scores.is_empty());
    }

    #[test]
    fn parse_rerank_response_out_of_range_number() {
        let snippets = vec![(0, "doc a")];
        let text = "5: 8\n";
        let scores = parse_rerank_response(text, &snippets);
        assert!(scores.is_empty());
    }

    #[test]
    fn parse_rerank_response_negative_scores() {
        let snippets = vec![(0, "doc a"), (1, "doc b")];
        let text = "1: -5\n2: 3\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 2);
        assert!((scores[0].relevance_score - 0.0).abs() < f32::EPSILON);
        assert!((scores[1].relevance_score - 0.3).abs() < f32::EPSILON);
    }

    #[test]
    fn parse_rerank_response_preserves_original_indices() {
        let snippets = vec![(42, "doc a"), (99, "doc b")];
        let text = "1: 10\n2: 0\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores[0].index, 42);
        assert_eq!(scores[1].index, 99);
    }

    #[test]
    fn parse_rerank_response_zero_score() {
        let snippets = vec![(0, "doc a")];
        let text = "1: 0\n";
        let scores = parse_rerank_response(text, &snippets);
        assert_eq!(scores.len(), 1);
        assert!((scores[0].relevance_score - 0.0).abs() < f32::EPSILON);
    }
}
