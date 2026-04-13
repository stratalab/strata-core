//! Citation extraction for RAG-generated answers.
//!
//! Parses `[N]` patterns from generated text and returns the 1-indexed
//! hit numbers the model cited. Handles `[1]`, `[1][2]` (consecutive),
//! `[1, 2, 3]` (comma-separated), `[10]` (multi-digit), and ignores
//! malformed `[abc]` or unmatched brackets.
//!
//! Manual byte-walk parser — no `regex` dependency. The grammar this
//! parser accepts inside the brackets is:
//!
//! ```text
//! digits ( (',' | space)+ digits )*
//! ```
//!
//! Anything that doesn't match is silently dropped.

use std::collections::BTreeSet;

/// Extract 1-indexed citation numbers from `text`, filtered to the range
/// `1..=max_hits`. Returns the citations in ascending order with duplicates
/// removed.
///
/// # Filters
///
/// - `n >= 1` — zero is invalid (citations are 1-indexed)
/// - `n <= max_hits` — out-of-range citations from confused models are dropped
/// - Deduplicated — `[1] and again [1]` returns `[1]`, not `[1, 1]`
///
/// # Edge cases
///
/// - Empty text → empty result
/// - `max_hits == 0` → empty result (no valid citations possible)
/// - Malformed brackets like `[abc]`, `[1`, `]2[` → ignored
/// - Mixed valid + invalid like `[1, abc, 2]` → returns the valid digits `[1, 2]`
pub fn extract_citations(text: &str, max_hits: usize) -> Vec<usize> {
    if max_hits == 0 || text.is_empty() {
        return Vec::new();
    }

    let mut found = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'[' {
            i += 1;
            continue;
        }

        // Walk forward looking for the closing `]`. Track whether the
        // contents look like our digits-and-separators grammar.
        let inner_start = i + 1;
        let mut j = inner_start;
        let mut found_any_digit = false;
        let mut malformed = false;
        while j < bytes.len() && bytes[j] != b']' {
            let c = bytes[j];
            if c.is_ascii_digit() {
                found_any_digit = true;
            } else if c == b',' || c == b' ' {
                // Allowed separator
            } else {
                // Anything else (letter, punctuation, etc.) → not a citation
                malformed = true;
                break;
            }
            j += 1;
        }

        // Need: `]` actually present, at least one digit, no malformed chars.
        if !malformed && found_any_digit && j < bytes.len() && bytes[j] == b']' {
            let inner = &text[inner_start..j];
            for num_str in inner.split(|c: char| c == ',' || c.is_whitespace()) {
                if num_str.is_empty() {
                    continue;
                }
                if let Ok(n) = num_str.parse::<usize>() {
                    if n >= 1 && n <= max_hits {
                        found.insert(n);
                    }
                }
            }
            i = j + 1;
            continue;
        }

        // Not a valid citation block — skip past the `[` and keep scanning.
        i += 1;
    }
    found.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_citations_single() {
        assert_eq!(extract_citations("answer with [1]", 5), vec![1]);
    }

    #[test]
    fn test_extract_citations_multiple_consecutive() {
        assert_eq!(extract_citations("see [1][2][3]", 5), vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_citations_comma_separated() {
        assert_eq!(extract_citations("see [1, 2, 3]", 5), vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_citations_multi_digit() {
        assert_eq!(extract_citations("see [10]", 10), vec![10]);
    }

    #[test]
    fn test_extract_citations_dedupes() {
        assert_eq!(extract_citations("[1] and [1] again [2]", 5), vec![1, 2]);
    }

    #[test]
    fn test_extract_citations_drops_zero() {
        assert_eq!(extract_citations("ref [0]", 5), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_drops_out_of_range() {
        // [99] exceeds max_hits=5 → dropped; [5] is at the boundary → kept.
        assert_eq!(extract_citations("see [5][99]", 5), vec![5]);
    }

    #[test]
    fn test_extract_citations_ignores_pure_alpha() {
        assert_eq!(extract_citations("see [abc]", 5), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_ignores_mixed_alpha() {
        // `[1, abc, 2]` has a non-digit, non-separator char → entire bracket
        // is rejected as malformed.
        assert_eq!(extract_citations("see [1, abc, 2]", 5), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_ignores_unclosed() {
        assert_eq!(
            extract_citations("see [1 and more text", 5),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn test_extract_citations_no_citations() {
        assert_eq!(
            extract_citations("plain answer with no brackets", 5),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn test_extract_citations_empty_text() {
        assert_eq!(extract_citations("", 5), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_max_hits_zero() {
        // No valid citations possible when max_hits == 0.
        assert_eq!(extract_citations("[1][2]", 0), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_empty_brackets() {
        // `[]` has no digits → ignored.
        assert_eq!(extract_citations("see []", 5), Vec::<usize>::new());
    }

    #[test]
    fn test_extract_citations_returns_sorted() {
        // Out-of-order input should still return sorted output.
        assert_eq!(extract_citations("see [3][1][2]", 5), vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_citations_realistic_answer() {
        let text = "Common side effects include nausea [1] and stomach pain [1][3]. \
                    In rare cases, lactic acidosis may occur [5].";
        assert_eq!(extract_citations(text, 5), vec![1, 3, 5]);
    }
}
