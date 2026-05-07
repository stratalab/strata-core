//! Output parser for query expansion responses.
//!
//! Parses `lex:`, `vec:`, and `hyde:` prefixed lines from model output.
//! Invalid lines are ignored.

use super::{ExpandedQueries, ExpandedQuery, QueryType};

/// Parse expansion output, filtering expansions that share no terms with the
/// original query.
///
/// When `original_query` is provided, each `lex:` and `vec:` expansion must
/// contain at least one word from the original query, case-insensitively. This
/// prevents semantic drift where a provider generates unrelated expansions.
///
/// `hyde:` lines are exempt because hypothetical documents may describe the
/// topic without using the exact query terms.
pub fn parse_expansion_with_filter(text: &str, original_query: Option<&str>) -> ExpandedQueries {
    let query_terms: Vec<String> = original_query
        .map(|q| q.split_whitespace().map(|t| t.to_lowercase()).collect())
        .unwrap_or_default();

    let mut queries = Vec::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (query_type, expansion_text) = if let Some(text) = trimmed.strip_prefix("lex:") {
            (QueryType::Lex, text.trim())
        } else if let Some(text) = trimmed.strip_prefix("vec:") {
            (QueryType::Vec, text.trim())
        } else if let Some(text) = trimmed.strip_prefix("hyde:") {
            (QueryType::Hyde, text.trim())
        } else {
            continue;
        };

        if expansion_text.is_empty() {
            continue;
        }

        if !query_terms.is_empty() && query_type != QueryType::Hyde {
            let expansion_lower = expansion_text.to_lowercase();
            let has_overlap = query_terms
                .iter()
                .any(|term| expansion_lower.contains(term.as_str()));
            if !has_overlap {
                continue;
            }
        }

        queries.push(ExpandedQuery {
            query_type,
            text: expansion_text.to_string(),
        });
    }

    ExpandedQueries { queries }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_output() {
        let text = "\
lex: user authentication login
vec: how does user authentication work in the system
hyde: The authentication module handles user login via OAuth2 tokens.";

        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 3);
        assert_eq!(result.queries[0].query_type, QueryType::Lex);
        assert_eq!(result.queries[0].text, "user authentication login");
        assert_eq!(result.queries[1].query_type, QueryType::Vec);
        assert_eq!(result.queries[2].query_type, QueryType::Hyde);
    }

    #[test]
    fn parse_multiple_lex() {
        let text = "\
lex: auth login
lex: oauth2 token
vec: authentication system overview";

        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 3);
        assert_eq!(result.queries[0].query_type, QueryType::Lex);
        assert_eq!(result.queries[1].query_type, QueryType::Lex);
        assert_eq!(result.queries[2].query_type, QueryType::Vec);
    }

    #[test]
    fn parse_ignores_invalid_lines() {
        let text = "\
This is some preamble the model should not output
lex: valid keyword query
Here is another invalid line
vec: valid semantic query
1. numbered list item
hyde: valid hypothetical document";

        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 3);
    }

    #[test]
    fn parse_empty_input() {
        let result = parse_expansion_with_filter("", None);
        assert!(result.queries.is_empty());
    }

    #[test]
    fn parse_all_invalid() {
        let text = "no valid prefixes here\njust garbage\n";
        let result = parse_expansion_with_filter(text, None);
        assert!(result.queries.is_empty());
    }

    #[test]
    fn parse_strips_whitespace() {
        let text = "  lex:  spaced out query  \n  vec:  another query  ";
        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 2);
        assert_eq!(result.queries[0].text, "spaced out query");
        assert_eq!(result.queries[1].text, "another query");
    }

    #[test]
    fn parse_skips_empty_text_after_prefix() {
        let text = "lex:\nvec: valid\nlex:   \nhyde: also valid";
        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 2);
        assert_eq!(result.queries[0].query_type, QueryType::Vec);
        assert_eq!(result.queries[1].query_type, QueryType::Hyde);
    }

    #[test]
    fn filter_keeps_expansions_with_overlap() {
        let text = "\
lex: user authentication login
vec: how does user login work
hyde: hypothetical doc about something else entirely";
        let result = parse_expansion_with_filter(text, Some("user authentication"));
        assert_eq!(result.queries.len(), 3);
    }

    #[test]
    fn filter_removes_drifted_lex_and_vec() {
        let text = "\
lex: completely unrelated topic
vec: nothing to do with query
hyde: hypothetical about deployment";
        let result = parse_expansion_with_filter(text, Some("user authentication"));
        assert_eq!(result.queries.len(), 1);
        assert_eq!(result.queries[0].query_type, QueryType::Hyde);
    }

    #[test]
    fn filter_case_insensitive() {
        let text = "lex: USER login methods";
        let result = parse_expansion_with_filter(text, Some("user authentication"));
        assert_eq!(result.queries.len(), 1);
    }

    #[test]
    fn no_filter_when_query_is_none() {
        let text = "lex: anything goes here";
        let result = parse_expansion_with_filter(text, None);
        assert_eq!(result.queries.len(), 1);
    }

    #[test]
    fn filter_partial_term_match() {
        let text = "lex: authentication flow";
        let result = parse_expansion_with_filter(text, Some("auth token"));
        assert_eq!(result.queries.len(), 1);
    }
}
