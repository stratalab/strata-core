//! Prompt template for reranking.

/// System prompt for relevance scoring via a model provider.
///
/// Instructs the provider to output `N: score` lines for each numbered document.
pub const SYSTEM_PROMPT: &str = "\
You are a search relevance scorer. Given a query and numbered documents, \
score each document's relevance to the query from 0 to 10.

Output format (one per line, no other text):
1: <score>
2: <score>
...

Rules:
- Score 0 = completely irrelevant, 10 = perfect match
- Output ONLY numbered score lines
- Score every document listed";

/// Provider-neutral prompt parts for reranking.
///
/// Provider adapters above engine decide how to map these parts into their
/// request format.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankPrompt {
    /// Instruction text for the model.
    pub system: &'static str,
    /// User prompt containing the query and numbered document snippets.
    pub user: String,
}

/// Build provider-neutral prompt parts for reranking.
pub fn build_prompt(query: &str, snippets: &[(usize, &str)]) -> RerankPrompt {
    let mut user_content = format!("Query: {}\n\nDocuments:", query);
    for (i, (_orig_idx, text)) in snippets.iter().enumerate() {
        user_content.push_str(&format!("\n{}. {}", i + 1, text));
    }

    RerankPrompt {
        system: SYSTEM_PROMPT,
        user: user_content,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_prompt_not_empty() {
        assert!(!SYSTEM_PROMPT.is_empty());
        assert!(SYSTEM_PROMPT.contains("score"));
        assert!(SYSTEM_PROMPT.contains("0 to 10"));
    }

    #[test]
    fn build_prompt_structure() {
        let snippets = vec![(0, "first doc"), (1, "second doc")];
        let prompt = build_prompt("test query", &snippets);
        assert_eq!(prompt.system, SYSTEM_PROMPT);
        assert!(prompt.user.contains("Query: test query"));
        assert!(prompt.user.contains("1. first doc"));
        assert!(prompt.user.contains("2. second doc"));
    }

    #[test]
    fn build_prompt_numbering() {
        let snippets = vec![(5, "alpha"), (10, "beta"), (15, "gamma")];
        let prompt = build_prompt("q", &snippets);
        assert!(prompt.user.contains("1. alpha"));
        assert!(prompt.user.contains("2. beta"));
        assert!(prompt.user.contains("3. gamma"));
    }
}
