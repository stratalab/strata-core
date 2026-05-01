//! Prompt construction for RAG generation.
//!
//! Two enhancements over the v0.6 spec are baked in:
//!
//! 1. **Sandwich ordering** (issue #2300): hits are reordered so the most
//!    relevant ones land at the start AND end of the context, with
//!    mid-relevance hits in the middle. Counters the "lost in the middle"
//!    effect where small models attend disproportionately to context
//!    boundaries.
//!
//! 2. **Type-tagged context** (issue #2302): each snippet is prefixed with
//!    its primitive type and identifier (e.g. `(kv 'doc1')`,
//!    `(json doc 'patient-4821')`, `(graph node 'g/n/x' [tenant_a])`).
//!    Helps the model reason about heterogeneous data and gives it a
//!    natural way to refer back to entities.
//!
//! Both are reversible if cross-model A/B testing shows them to be
//! neutral or harmful.

use strata_core::EntityRef;
use strata_engine::search::SearchHit;

/// Build the full RAG prompt: system instructions + sandwich-ordered,
/// type-tagged context block + user query, formatted with the Qwen-style
/// chat template that strata-inference's local backend expects.
///
/// Returns `None` if the context budget is so tight that not even one
/// snippet fits — extremely unlikely for typical inputs.
pub fn build_rag_prompt(
    system_prompt: &str,
    hits: &[SearchHit],
    query: &str,
    max_answer_tokens: usize,
) -> Option<String> {
    let context = build_context_block(hits, max_answer_tokens)?;
    Some(format!(
        "<|im_start|>system\n{system}<|im_end|>\n\
         <|im_start|>user\n{context}\n\
         Question: {query}<|im_end|>\n\
         <|im_start|>assistant\n",
        system = system_prompt,
        context = context,
        query = query,
    ))
}

/// Reorder hits so the top hit lands at the start and the second-most
/// relevant lands at the end, with mid-relevance hits in the middle in
/// their original retrieval order. Implements issue #2300 (sandwich
/// ordering / lost-in-the-middle defense).
///
/// The "sandwich" name comes from the fact that the most attention-worthy
/// content lives at the boundaries of the context where small models
/// attend most. The middle hits are still present but get less attention,
/// which is acceptable because they're lower-relevance anyway.
///
/// # Examples
///
/// - `[a, b, c, d, e]` (a most relevant) → `[a, c, d, e, b]`
/// - `[a, b, c, d]` → `[a, c, d, b]`
/// - `[a, b, c]` → `[a, c, b]`
/// - `[a, b]` → `[a, b]` (no-op for length ≤ 2)
/// - `[a]` → `[a]`
/// - `[]` → `[]`
pub(crate) fn sandwich_reorder<T: Clone>(hits: &[T]) -> Vec<T> {
    if hits.len() <= 2 {
        return hits.to_vec();
    }
    // Top hit first, mid hits in retrieval order, second-best last.
    let mut out = Vec::with_capacity(hits.len());
    out.push(hits[0].clone());
    for h in &hits[2..] {
        out.push(h.clone());
    }
    out.push(hits[1].clone());
    out
}

/// Format a single hit as a numbered, type-tagged context line.
/// Implements issue #2302 (type-tagged context for heterogeneous data).
///
/// Output shape: `[N] (<type> '<id>'[ [<space>]]) <snippet>`
///
/// The space tag is omitted when the hit is in the default space, to keep
/// the prompt compact for single-tenant deployments.
pub(crate) fn format_hit(index: usize, hit: &SearchHit) -> String {
    let snippet = hit.snippet.as_deref().unwrap_or("(no snippet)");
    let type_tag = match &hit.doc_ref {
        EntityRef::Kv { space, key, .. } => {
            if space == "default" {
                format!("kv '{}'", key)
            } else {
                format!("kv '{}' [{}]", key, space)
            }
        }
        EntityRef::Json { space, doc_id, .. } => {
            if space == "default" {
                format!("json doc '{}'", doc_id)
            } else {
                format!("json doc '{}' [{}]", doc_id, space)
            }
        }
        EntityRef::Graph { space, key, .. } => {
            if space == "default" {
                format!("graph node '{}'", key)
            } else {
                format!("graph node '{}' [{}]", key, space)
            }
        }
        EntityRef::Event {
            sequence, space, ..
        } => {
            if space == "default" {
                format!("event #{}", sequence)
            } else {
                format!("event #{} [{}]", sequence, space)
            }
        }
        EntityRef::Vector {
            collection, key, ..
        } => format!("vector '{}/{}'", collection, key),
        EntityRef::Branch { .. } => "branch".to_string(),
    };
    format!("[{}] ({}) {}", index, type_tag, snippet)
}

/// Build the context block: sandwich-ordered, type-tagged snippets, with
/// budget-aware truncation from the bottom of the ordered list. The
/// numbering is 1-indexed and reflects the **sandwich-ordered** position,
/// so `[1]` is the most relevant hit and `[N]` is the second-most relevant.
fn build_context_block(hits: &[SearchHit], max_answer_tokens: usize) -> Option<String> {
    if hits.is_empty() {
        return None;
    }
    let ordered = sandwich_reorder(hits);

    // Conservative token budget. Real models have very different context
    // sizes (Qwen 1.7B = 2K, Claude/Gemini = 200K+), but we target the
    // smallest reasonable target so the prompt works everywhere. The
    // 4 chars/token estimate is approximate but safe.
    const MODEL_CONTEXT_TOKENS: usize = 8192;
    const OVERHEAD_TOKENS: usize = 200; // system prompt + template + query
    const CHARS_PER_TOKEN: usize = 4;

    let reserved_tokens = OVERHEAD_TOKENS.saturating_add(max_answer_tokens);
    let available_tokens = MODEL_CONTEXT_TOKENS.saturating_sub(reserved_tokens);
    let available_chars = available_tokens.saturating_mul(CHARS_PER_TOKEN);

    let header = "Context:\n";
    let mut context = String::from(header);
    let mut consumed_chars = header.len();
    let mut included = 0usize;

    for (i, hit) in ordered.iter().enumerate() {
        let line = format_hit(i + 1, hit);
        let line_chars = line.len() + 1; // newline
        if consumed_chars + line_chars > available_chars && included > 0 {
            break; // Drop from the bottom of the sandwich-ordered list
        }
        context.push_str(&line);
        context.push('\n');
        consumed_chars += line_chars;
        included += 1;
    }

    if included == 0 {
        return None;
    }
    Some(context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;
    use strata_engine::search::SearchHit;

    fn kv_hit(key: &str, space: &str, snippet: &str, score: f32) -> SearchHit {
        SearchHit {
            doc_ref: EntityRef::Kv {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: space.into(),
                key: key.into(),
            },
            score,
            rank: 0,
            snippet: Some(snippet.into()),
        }
    }

    fn json_hit(doc_id: &str, space: &str, snippet: &str) -> SearchHit {
        SearchHit {
            doc_ref: EntityRef::Json {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: space.into(),
                doc_id: doc_id.into(),
            },
            score: 0.5,
            rank: 0,
            snippet: Some(snippet.into()),
        }
    }

    fn graph_hit(key: &str, space: &str, snippet: &str) -> SearchHit {
        SearchHit {
            doc_ref: EntityRef::Graph {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: space.into(),
                key: key.into(),
            },
            score: 0.5,
            rank: 0,
            snippet: Some(snippet.into()),
        }
    }

    // ---- sandwich_reorder ----

    #[test]
    fn test_sandwich_reorder_5_hits() {
        let input = vec!["a", "b", "c", "d", "e"];
        // Top hit (a) first, mid hits (c, d, e) in retrieval order, second-best (b) last.
        assert_eq!(sandwich_reorder(&input), vec!["a", "c", "d", "e", "b"]);
    }

    #[test]
    fn test_sandwich_reorder_4_hits() {
        let input = vec!["a", "b", "c", "d"];
        // a first, c+d in middle, b last.
        assert_eq!(sandwich_reorder(&input), vec!["a", "c", "d", "b"]);
    }

    #[test]
    fn test_sandwich_reorder_3_hits() {
        let input = vec!["a", "b", "c"];
        // a first, c in middle, b last.
        assert_eq!(sandwich_reorder(&input), vec!["a", "c", "b"]);
    }

    #[test]
    fn test_sandwich_reorder_2_hits() {
        // No-op for length ≤ 2 — sandwich would be a no-op anyway.
        let input: &[&str] = &["a", "b"];
        assert_eq!(sandwich_reorder(input), vec!["a", "b"]);
    }

    #[test]
    fn test_sandwich_reorder_1_hit() {
        let input: &[&str] = &["a"];
        assert_eq!(sandwich_reorder(input), vec!["a"]);
    }

    #[test]
    fn test_sandwich_reorder_empty() {
        let v: &[&str] = &[];
        assert_eq!(sandwich_reorder(v), Vec::<&str>::new());
    }

    #[test]
    fn test_sandwich_reorder_puts_top_two_at_boundaries() {
        // Property: top hit at position 0, second-best at the last position,
        // for any input of length ≥ 3.
        for n in 3..=10 {
            let input: Vec<usize> = (0..n).collect();
            let out = sandwich_reorder(&input);
            assert_eq!(out[0], 0, "top hit should be at position 0 (n={})", n);
            assert_eq!(
                out[out.len() - 1],
                1,
                "second-best hit should be at the last position (n={}, out={:?})",
                n,
                out
            );
        }
    }

    #[test]
    fn test_sandwich_reorder_preserves_middle_order() {
        // The middle hits (rank 2..N-1 in retrieval order) should appear in
        // their original retrieval order, not reversed or shuffled.
        let input = vec![0, 1, 2, 3, 4, 5, 6];
        let out = sandwich_reorder(&input);
        // Expected: [0, 2, 3, 4, 5, 6, 1]
        assert_eq!(out, vec![0, 2, 3, 4, 5, 6, 1]);
    }

    // ---- format_hit ----

    #[test]
    fn test_format_hit_kv_default_space() {
        let hit = kv_hit("doc1", "default", "the content", 0.9);
        assert_eq!(format_hit(1, &hit), "[1] (kv 'doc1') the content");
    }

    #[test]
    fn test_format_hit_kv_tenant_space() {
        let hit = kv_hit("doc1", "tenant_a", "the content", 0.9);
        assert_eq!(
            format_hit(1, &hit),
            "[1] (kv 'doc1' [tenant_a]) the content"
        );
    }

    #[test]
    fn test_format_hit_json_default_space() {
        let hit = json_hit("patient-4821", "default", "{\"name\":\"alice\"}");
        assert_eq!(
            format_hit(2, &hit),
            "[2] (json doc 'patient-4821') {\"name\":\"alice\"}"
        );
    }

    #[test]
    fn test_format_hit_json_tenant_space() {
        let hit = json_hit("patient-4821", "tenant_b", "{\"name\":\"bob\"}");
        assert_eq!(
            format_hit(2, &hit),
            "[2] (json doc 'patient-4821' [tenant_b]) {\"name\":\"bob\"}"
        );
    }

    #[test]
    fn test_format_hit_graph_default_space() {
        let hit = graph_hit("mygraph/n/node1", "default", "Patient: Alice, age 42");
        assert_eq!(
            format_hit(3, &hit),
            "[3] (graph node 'mygraph/n/node1') Patient: Alice, age 42"
        );
    }

    #[test]
    fn test_format_hit_graph_tenant_space() {
        let hit = graph_hit("mygraph/n/node1", "tenant_a", "Patient: Alice, age 42");
        assert_eq!(
            format_hit(3, &hit),
            "[3] (graph node 'mygraph/n/node1' [tenant_a]) Patient: Alice, age 42"
        );
    }

    #[test]
    fn test_format_hit_vector() {
        // Vector hits are unusual in search results today but the format
        // path exists. Verify the (collection, key) tag renders.
        let hit = SearchHit {
            doc_ref: EntityRef::Vector {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: "default".into(),
                collection: "embeddings".into(),
                key: "doc-42".into(),
            },
            score: 0.5,
            rank: 0,
            snippet: Some("vector snippet".into()),
        };
        assert_eq!(
            format_hit(5, &hit),
            "[5] (vector 'embeddings/doc-42') vector snippet"
        );
    }

    #[test]
    fn test_format_hit_event() {
        let hit = SearchHit {
            doc_ref: EntityRef::Event {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: "default".into(),
                sequence: 42,
            },
            score: 0.5,
            rank: 0,
            snippet: Some("login alice".into()),
        };
        assert_eq!(format_hit(4, &hit), "[4] (event #42) login alice");
    }

    #[test]
    fn test_format_hit_no_snippet() {
        let hit = SearchHit {
            doc_ref: EntityRef::Kv {
                branch_id: BranchId::from_bytes([0u8; 16]),
                space: "default".into(),
                key: "doc1".into(),
            },
            score: 0.9,
            rank: 0,
            snippet: None,
        };
        assert_eq!(format_hit(1, &hit), "[1] (kv 'doc1') (no snippet)");
    }

    // ---- build_rag_prompt ----

    #[test]
    fn test_build_rag_prompt_includes_system_and_query() {
        let hits = vec![kv_hit("doc1", "default", "rust is a systems language", 0.9)];
        let prompt = build_rag_prompt(
            "Answer using only the provided context.",
            &hits,
            "What is Rust?",
            500,
        )
        .expect("prompt should build");
        assert!(prompt.contains("Answer using only the provided context."));
        assert!(prompt.contains("Question: What is Rust?"));
        assert!(prompt.contains("rust is a systems language"));
        assert!(prompt.starts_with("<|im_start|>system"));
        assert!(prompt.ends_with("<|im_start|>assistant\n"));
    }

    #[test]
    fn test_build_rag_prompt_uses_sandwich_order() {
        // Three hits in retrieval order. Sandwich order should put the
        // most relevant first AND the second-most relevant near the end.
        let hits = vec![
            kv_hit("top", "default", "ALPHA most relevant", 0.99),
            kv_hit("mid", "default", "BETA mid relevance", 0.5),
            kv_hit("low", "default", "GAMMA least relevant", 0.1),
        ];
        let prompt = build_rag_prompt("system", &hits, "Q?", 500).expect("prompt should build");

        // ALPHA must appear before GAMMA, and GAMMA must appear before BETA.
        // (Sandwich order on 3 hits: [top, low, mid] → [a,c,b] in test_sandwich_reorder_3_hits.)
        let alpha_pos = prompt.find("ALPHA").expect("ALPHA in prompt");
        let beta_pos = prompt.find("BETA").expect("BETA in prompt");
        let gamma_pos = prompt.find("GAMMA").expect("GAMMA in prompt");
        assert!(alpha_pos < gamma_pos, "top hit should come before low hit");
        assert!(
            gamma_pos < beta_pos,
            "mid hit should come last (sandwich pattern)"
        );
    }

    #[test]
    fn test_build_rag_prompt_empty_hits_returns_none() {
        let hits: Vec<SearchHit> = vec![];
        // Spec: zero-hits canned response is the caller's responsibility;
        // build_rag_prompt itself returns None for empty input.
        let prompt = build_rag_prompt("system", &hits, "Q?", 500);
        assert!(prompt.is_none());
    }

    #[test]
    fn test_build_rag_prompt_truncates_when_over_budget() {
        // Construct enough hits to blow the ~32K-char budget. With a tight
        // max_answer_tokens budget the available context shrinks.
        let big_snippet = "x".repeat(2000);
        let hits: Vec<SearchHit> = (0..50)
            .map(|i| kv_hit(&format!("doc{}", i), "default", &big_snippet, 0.5))
            .collect();
        // 50 × 2000 chars = 100K chars of snippet alone. Way over budget.
        // Reserve enough answer tokens that very few snippets fit.
        let prompt = build_rag_prompt("system", &hits, "Q?", 1500)
            .expect("prompt should build at least one hit");
        // Should not contain all 50 hits — the bottom of the sandwich is dropped.
        let included = (0..50)
            .filter(|i| prompt.contains(&format!("doc{}", i)))
            .count();
        assert!(included < 50, "expected truncation; got {} hits", included);
        assert!(included > 0, "at least one hit should fit");
    }
}
