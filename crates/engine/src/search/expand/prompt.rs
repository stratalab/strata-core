//! Prompt template for query expansion.

/// System prompt for query expansion via a model provider.
///
/// Instructs the provider to output `lex:`, `vec:`, and `hyde:` prefixed lines.
pub const SYSTEM_PROMPT: &str = "\
You are a search query expander for a multi-primitive database that stores \
key-value pairs, JSON documents, events, state cells, and vector embeddings.

Given a user's search query, generate alternative search variants to improve recall.

Output format (one per line, no other text):
lex: <keyword query for BM25 text search>
vec: <natural language phrase for semantic vector similarity>
hyde: <hypothetical document passage that would match the query>

Rules:
- Generate 1-3 lex lines (keyword reformulations, abbreviations, technical terms)
- Generate 1 vec line (natural language rephrasing for semantic similarity)
- Generate 1 hyde line (50-200 chars, what the ideal matching document looks like)
- Do NOT repeat the original query verbatim
- Do NOT include any explanation, numbering, or markdown
- Output ONLY lines starting with lex:, vec:, or hyde:";

/// Provider-neutral prompt parts for query expansion.
///
/// Provider adapters above engine decide how to map these parts into their
/// request format.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpansionPrompt {
    /// Instruction text for the model.
    pub system: &'static str,
    /// User query to expand.
    pub user: String,
}

/// Build provider-neutral prompt parts for query expansion.
pub fn build_prompt(query: &str) -> ExpansionPrompt {
    ExpansionPrompt {
        system: SYSTEM_PROMPT,
        user: query.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_prompt_not_empty() {
        assert!(!SYSTEM_PROMPT.is_empty());
        assert!(SYSTEM_PROMPT.contains("lex:"));
        assert!(SYSTEM_PROMPT.contains("vec:"));
        assert!(SYSTEM_PROMPT.contains("hyde:"));
    }

    #[test]
    fn build_prompt_structure() {
        let prompt = build_prompt("test query");
        assert_eq!(prompt.system, SYSTEM_PROMPT);
        assert_eq!(prompt.user, "test query");
    }
}
