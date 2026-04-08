# RAG Architecture in Strata

This document describes the **as-shipped** RAG (retrieval-augmented generation)
implementation that landed in v0.6. It supersedes the pre-implementation
planning notes in `v0.6-implementation.md` (kept in git history for reference)
and is intended to remain the canonical RAG reference as v0.7+ extends the
feature.

For *why* RAG is in Strata at all, see `intelligence-layer.md` and
`search-strategy.md`. This document is about *how* it works.

---

## 1. Goals

RAG in Strata is one call:

```python
db.search("What are the side effects of metformin?", recipe="rag")
```

returns hits *and* a grounded answer with citations, against an embedded
database, with no external infrastructure. Local models work out of the
box. Cloud providers (Anthropic, OpenAI, Google) work with the same call
when their API keys are set.

The implementation is structured to support **A/B comparing answer quality
across models** (Qwen 1.7B vs Claude vs Gemini) by swapping
`recipe.models.generate` — the user's stated v0.6 goal.

### Non-goals (deferred to later versions)

- **Multi-model routing** (rules, fallback chains, cost-aware selection) —
  v0.6 reads `models.generate` and calls one model. Routing is v0.8.
- **Streaming answers** — answers come back as a single string. Issue #2301.
- **Answer caching** — every call hits the model. Issue #2303.
- **Adaptive RAG routing** — RAG runs whenever the recipe has `prompt` set.
  No "is this query simple enough to skip retrieval?" logic. Issue #2304.
- **Per-response quality signals** (faithfulness scores, hallucination
  detection). Issue #2305.
- **Prompt injection defense** — snippets are not sanitized before injection.
  Issue #2306.
- **Conversation / multi-turn context** — every call is independent.

---

## 2. Where RAG sits in the stack

Strata's search pipeline has three layers:

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 3: Intelligence (strata-intelligence)                     │
│  - expansion          (v0.2)                                      │
│  - rerank             (v0.2)                                      │
│  - rag  ← v0.6        (this doc)                                  │
└──────────────────────────────────────────────────────────────────┘
                              ▲
                              │ used by
                              │
┌──────────────────────────────────────────────────────────────────┐
│  Layer 2: Executor (strata-executor)                             │
│  - handlers/search.rs orchestrates substrate + intelligence      │
│  - projects intelligence-layer types into wire types             │
└──────────────────────────────────────────────────────────────────┘
                              ▲
                              │ used by
                              │
┌──────────────────────────────────────────────────────────────────┐
│  Layer 1: Substrate (strata-search + strata-engine)              │
│  - retrieval, fusion, filtering, ranked SearchHits               │
└──────────────────────────────────────────────────────────────────┘
```

RAG is **layer 3**. It runs *after* the substrate has produced ranked hits
and *after* reranking. It does not change the substrate. It does not change
how hits are ranked. It takes the top-N hits as input and produces an
answer string + citation list as output.

The executor is the only call site that wires the intelligence layer's
`generate_answer` into a `SearchResults` response. The substrate has zero
knowledge that RAG exists.

---

## 3. The trigger gate

> **RAG runs when `recipe.prompt.is_some()`.**

There is no `mode` field on `SearchQuery`. There is no `enable_rag` flag.
The recipe is the single source of truth, and a recipe with a `prompt`
field opts into RAG. This mirrors how every other intelligence-layer
feature is gated:

| Feature   | Gate                          |
|-----------|-------------------------------|
| Expansion | `recipe.expansion.is_some()`  |
| Rerank    | `recipe.rerank.is_some()`     |
| RAG       | `recipe.prompt.is_some()`     |

The default recipe (`builtin_defaults()`) does not set `prompt`, so RAG
is disabled by default. Behavior with the default recipe is bit-identical
to pre-v0.6.

The `"rag"` builtin recipe at `crates/engine/src/search/recipe.rs:551-608`
sets all four RAG fields and is what users select to opt in:

```rust
prompt: Some("Answer using only the provided context. Cite sources with [N]."),
rag_context_hits: Some(5),
rag_max_tokens: Some(500),
models: Some(ModelsConfig {
    embed:    Some("local:miniLM".into()),
    expand:   Some("local:qwen3:1.7b".into()),
    rerank:   Some("local:jina-reranker-v1-tiny".into()),
    generate: Some("anthropic:claude-sonnet-4-6".into()),
}),
```

Users can also override via inline JSON:

```python
db.search(
    "...",
    recipe={"prompt": "...", "models": {"generate": "local:qwen3:1.7b"}},
)
```

---

## 4. Module layout

```
crates/intelligence/src/rag/
├── mod.rs         — public API: generate_answer(db, query, hits, recipe) → Option<RagAnswer>
├── prompt.rs      — sandwich ordering, type-tagged context, token budget
└── citation.rs    — manual byte-walk parser for [N] citations
```

The whole module is gated `#[cfg(feature = "embed")]` and is a thin
adapter on top of the v0.2 model loader (`GenerateModelState` in
`crates/intelligence/src/generate.rs`).

The executor side has a single insertion point at
`crates/executor/src/handlers/search.rs:170` and a cfg-gated shim helper
that mirrors the existing `try_expand_query` / `rerank_hits` pattern.

---

## 5. Generation flow

```
db.search("What are the side effects of metformin?", recipe="rag")
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. Substrate (unchanged from v0.5)                              │
│    BM25 + vector + expansion + rerank → ranked SearchHits       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼  final_hits: Vec<SearchHit>
  │
┌─────────────────────────────────────────────────────────────────┐
│ 2. handlers/search.rs:170                                       │
│    let rag_answer = generate_rag_answer(p, &sq.query,           │
│                                          &final_hits,           │
│                                          &resolved);            │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. intelligence/rag/mod.rs::generate_answer                     │
│    a. Trigger gate: recipe.prompt.as_deref()? → bail if None    │
│    b. Resolve model spec (recipe.models.generate or default)    │
│    c. If hits.is_empty() → return canned response, skip model   │
│    d. Build sandwich-ordered, type-tagged prompt                │
│    e. Lazy-load model via GenerateModelState                    │
│    f. Call engine.generate(prompt, max_tokens, ...)             │
│    g. Extract [N] citations from response.text                  │
│    h. Return RagAnswer { text, sources, model, elapsed, tokens }│
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. handlers/search.rs:250 — projection                          │
│    Split RagAnswer into:                                        │
│      Output::SearchResults.answer  → AnswerResponse{text,srcs}  │
│      stats.rag_used / rag_model / rag_elapsed_ms /              │
│            rag_tokens_in / rag_tokens_out                       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
Output::SearchResults { hits, stats, diff, answer }
```

Failure at any step in (3) returns `None` and the user gets hits without
an answer — graceful degradation, never an error to the caller.

---

## 6. Prompt construction

Two enhancements over the v0.6 spec are baked in.

### 6.1 Sandwich ordering (issue #2300)

Hits are reordered so the **top hit lands at the start** and the
**second-most-relevant hit lands at the end** of the context, with
mid-relevance hits in the middle in their original retrieval order.
This counters the "lost in the middle" effect where small models attend
disproportionately to context boundaries.

Examples:
- `[a, b, c, d, e]` → `[a, c, d, e, b]`
- `[a, b, c, d]`    → `[a, c, d, b]`
- `[a, b, c]`       → `[a, c, b]`
- `[a, b]`          → `[a, b]`  (no-op for length ≤ 2)

The numbering in the prompt reflects sandwich position, so `[1]` in the
model's answer always refers to the most-relevant hit and `[N]` to the
second-most-relevant.

A property test asserts the boundary invariant for all `n ∈ 3..=10`:
```rust
assert_eq!(out[0], 0);                  // top hit at front
assert_eq!(out[out.len() - 1], 1);      // second-best at back
```

### 6.2 Type-tagged context (issue #2302)

Each snippet is prefixed with its primitive type and identifier so
the model can reason about heterogeneous data:

| Variant                       | Format                                       |
|-------------------------------|----------------------------------------------|
| `Kv` in default space         | `[1] (kv 'doc1') the content`                |
| `Kv` in tenant space          | `[1] (kv 'doc1' [tenant_a]) the content`     |
| `Json`                        | `[2] (json doc 'patient-4821') {...}`        |
| `Graph`                       | `[3] (graph node 'mygraph/n/node1') ...`     |
| `Event`                       | `[4] (event #42) login alice`                |
| `Vector`                      | `[5] (vector 'embeddings/doc-42') ...`       |

The space tag is omitted for `default` to keep the prompt compact for
single-tenant deployments.

### 6.3 Chat template

The full prompt is wrapped with Qwen-style chat markers (the local
backend's expected format; cloud providers ignore them harmlessly):

```
<|im_start|>system
{system_prompt}<|im_end|>
<|im_start|>user
Context:
[1] (kv 'doc1') ...
[2] (json doc 'patient-4821') ...
[3] (graph node 'mygraph/n/x') ...

Question: {user_query}<|im_end|>
<|im_start|>assistant
```

### 6.4 Token budget

The budget is conservative because the smallest target model (Qwen 1.7B)
has a 2K context window, while cloud models have 200K+:

| Constant              | Value | Meaning                                    |
|-----------------------|-------|--------------------------------------------|
| `MODEL_CONTEXT_TOKENS`| 8192  | Floor for context window planning          |
| `OVERHEAD_TOKENS`     | 200   | System prompt + template + query reserve   |
| `CHARS_PER_TOKEN`     | 4     | Approximate, safe estimate                 |

```
available_tokens = 8192 - (200 + max_answer_tokens)
available_chars  = available_tokens × 4
```

When the running prompt exceeds `available_chars`, the loop breaks and
drops the remaining hits — but **only after the first hit is included**
unconditionally, so a degenerate budget never produces zero context.

Because hits are sandwich-ordered before truncation, the **mid-relevance
hits get dropped first**, preserving the top hit at the front and the
second-best at the back. The boundary invariants hold even under
truncation.

`OVERHEAD_TOKENS.saturating_add(max_answer_tokens)` is used to avoid
debug-mode overflow if a user passes a pathological `max_answer_tokens`.

---

## 7. Citation extraction

`extract_citations(text, max_hits) -> Vec<usize>` parses `[N]` patterns
from the generated text and returns 1-indexed hit numbers in ascending
order with duplicates removed.

Implementation is a manual byte-walk parser, not regex — no dependency
needed for the small grammar:

```
inner = digits ( (',' | space)+ digits )*
```

Anything that doesn't match is silently dropped.

### Filters

- `n >= 1` — zero is invalid (citations are 1-indexed).
- `n <= max_hits` — out-of-range citations from confused models are dropped.
- Deduplicated via `BTreeSet`, returned in ascending order.

### Edge cases handled

| Input                   | Result                            |
|-------------------------|-----------------------------------|
| `"[1]"`                 | `[1]`                             |
| `"[1][2][3]"`           | `[1, 2, 3]`                       |
| `"[1, 2, 3]"`           | `[1, 2, 3]`                       |
| `"[10]"` with max=10    | `[10]`                            |
| `"[1] and [1] and [2]"` | `[1, 2]` (dedup)                  |
| `"[0]"`                 | `[]`  (zero invalid)              |
| `"[5][99]"` with max=5  | `[5]`  (drops out-of-range)       |
| `"[abc]"`               | `[]`  (no digits)                 |
| `"[1, abc, 2]"`         | `[]`  (mixed alpha rejects bracket)|
| `"[1 unclosed"`         | `[]`                              |
| `"[]"`                  | `[]`                              |
| `""`                    | `[]`                              |
| `max_hits == 0`         | `[]`  (no valid range)            |

The byte-walk is safe under multi-byte UTF-8: `[` (0x5B) is ASCII and
never appears as a continuation byte in any valid UTF-8 sequence, so
the loop cannot accidentally split a multi-byte character.

---

## 8. Multi-model support

`recipe.models.generate: Option<String>` selects the model. The string is
passed verbatim to the strata-inference loader, which handles both local
and cloud providers uniformly:

| Spec                            | Provider          |
|---------------------------------|-------------------|
| `"local:qwen3:1.7b"`            | Local GGUF        |
| `"anthropic:claude-sonnet-4-6"` | Anthropic API     |
| `"openai:gpt-4o-mini"`          | OpenAI API        |
| `"google:gemini-pro"`           | Google API        |

Default when unset: `"local:qwen3:1.7b"`.

The same `GenerateModelState::get_or_load(model_spec)` extension that v0.2
uses for query expansion handles RAG too. Models are loaded lazily on the
first request, cached on the database extension, and reused across calls.

Sampling parameters (`temperature: 0.3`, `top_k: 40`, `top_p: 0.9`) are
hardcoded in v0.6 to favor factual, grounded answers over creative ones.
RAG is not the same shape as expansion (where 0.7 makes sense for diverse
reformulations).

What v0.6 does **not** do:

- No fallback chains. If `models.generate` is set to Claude and the API is
  down, the call returns `None` — it does not retry locally. Routing is v0.8.
- No cost-aware selection. The user picks the model.
- No per-call overrides on temperature/top_k/top_p. v0.7+ may add these
  if A/B testing surfaces a real need.

---

## 9. Graceful degradation

RAG must never block search. If anything goes wrong, the user gets hits
without an answer — never an error.

| Failure mode                       | Behavior                              |
|------------------------------------|---------------------------------------|
| `recipe.prompt` is `None`          | RAG skipped, hits returned, answer=None |
| Zero hits from substrate           | Canned response, model not called     |
| Model load fails (registry/HTTP)   | tracing::warn, return None            |
| Generation fails (timeout, error)  | tracing::warn, return None            |
| Engine threading error             | tracing::warn, return None            |

Zero-hits canned response:

```
"I don't have enough information to answer this question."
```

This is intentional — the user asked a question, the index returned
nothing, and forwarding "no context" to a model wastes a call. The canned
text is consistent with what well-prompted models say in the same
situation, so the UX is unchanged from the user's perspective.

In every failure case, `tracing::warn!(target: "strata::rag", ...)` emits
the cause for operators to debug.

---

## 10. Wire types

### `Output::SearchResults`

```rust
SearchResults {
    hits: Vec<SearchResultHit>,
    stats: SearchStatsOutput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    diff: Option<DiffOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    answer: Option<AnswerResponse>,    // ← v0.6
}
```

### `AnswerResponse`

```rust
pub struct AnswerResponse {
    pub text: String,
    pub sources: Vec<usize>,  // 1-indexed hit numbers cited in text
}
```

`sources[i] - 1` is the index into the `hits` array of the same payload —
the consumer can join the answer text to the underlying entities.

### `SearchStatsOutput` RAG fields

```rust
pub rag_used:        Option<bool>,    // Some(true) only on success
pub rag_model:       Option<String>,
pub rag_elapsed_ms:  Option<f64>,
pub rag_tokens_in:   Option<u32>,     // prompt tokens
pub rag_tokens_out:  Option<u32>,     // completion tokens
```

`rag_used` is `Some(true)` only when generation succeeded. `None` covers
both "not attempted" (no `prompt` in recipe) and "attempted but failed".
This matches the existing `expansion_used` / `rerank_used` semantics —
operators consult traces for failure causes.

`rag_elapsed_ms == 0.0` is the marker for the canned-response path
(zero hits, model not called).

All RAG fields use `#[serde(default, skip_serializing_if = "Option::is_none")]`
so existing payloads round-trip without the new fields populated.

---

## 11. CLI rendering

The human-mode formatter at `crates/cli/src/format.rs:1090` surfaces the
answer at the top of the output, with the citation index as the hit list
below:

```
=== answer (anthropic:claude-sonnet-4-6, 840ms) ===
Common side effects of metformin include nausea, diarrhea, and stomach
pain [1]. In rare cases, lactic acidosis may occur, particularly in
elderly patients with kidney problems [1].
sources: [1]


1) "default/metformin" [kv] (score: 0.920) - Common side effects of metformin...
2) "default/ozempic"   [kv] (score: 0.510) - Ozempic is a glucose-lowering...

--- stats ---
mode: hybrid, elapsed: 850.0ms, candidates: 2, index: true,
truncated: false, expansion: false, rerank: false,
rag: true (anthropic:claude-sonnet-4-6)
```

When the recipe does not enable RAG, the answer block is omitted and
the stats footer drops the `rag: true` segment. JSON mode (`--json`)
includes the answer via serde derive automatically. Raw mode (`--raw`)
intentionally omits the answer because tabular output is for scripts.

---

## 12. Tests

### Unit tests (intelligence layer)

`crates/intelligence/src/rag/`:

| Module       | Tests | Coverage                                       |
|--------------|-------|------------------------------------------------|
| `mod.rs`     | 4     | trigger gate, canned response, model load fail |
| `prompt.rs`  | 21    | sandwich for n=0..7, format_hit per variant, build_rag_prompt, truncation |
| `citation.rs`| 16    | every documented edge case + realistic answer  |

The sandwich property test asserts the boundary invariant for `n ∈ 3..=10`,
catching algorithmic regressions across all reasonable input sizes.

### Wire-type round-trips

`crates/executor/src/tests/serialization.rs::test_output_search_results_with_rag_answer`
round-trips a populated `SearchResults` with a real `AnswerResponse` and
all RAG stats fields, verifying nothing drops on the wire.

### Executor integration tests

`crates/executor/src/tests/search.rs`:

| Test                                            | What it proves                                                |
|-------------------------------------------------|---------------------------------------------------------------|
| `test_search_default_recipe_no_rag_answer`      | Default recipe must not invoke RAG. All `rag_*` fields None.  |
| `test_search_rag_recipe_zero_hits_canned_response` | Inline RAG recipe + empty index → canned answer with model name + 0ms elapsed. Exercises full executor wiring without loading a real model. |

The zero-hits path is the only RAG integration test that runs without
model infrastructure, because the canned response short-circuits before
the model loader is touched. Tests that exercise real generation are
manual smoke tests, not CI.

### CLI rendering tests

`crates/cli/src/format.rs::tests`:

| Test                                                    | What it proves                                |
|---------------------------------------------------------|-----------------------------------------------|
| `test_format_search_results_with_rag_answer_human`      | Answer header, body, sources, hits, and `rag: true (model)` footer all render. |
| `test_format_search_results_without_rag_answer_human`   | Default search omits the `=== answer ===` header and `rag: true` footer.       |

### Manual smoke tests (not in CI)

These require model infrastructure:

1. **Local Qwen** — `recipe.models.generate = "local:qwen3:1.7b"`. Verify
   citations land on real hits and the model refuses when context is thin.
2. **Cloud Claude** — `ANTHROPIC_API_KEY` set, default `"rag"` recipe.
   Same query as Qwen run. Compare answer quality and citation accuracy.
3. **Cloud Gemini** — `GOOGLE_API_KEY` set, override
   `models.generate = "google:gemini-pro"`. Three-way comparison.
4. **Multi-primitive** — Index across KV + JSON + Graph. Verify the
   type-tagged context shows all three and the model's citations land
   on the right primitives.
5. **Sandwich verification** — Inject a unique anchor phrase into the
   top hit and confirm the model latches onto it (front-of-sandwich).

---

## 13. File reference

| File                                                          | Role                                                  |
|---------------------------------------------------------------|-------------------------------------------------------|
| `crates/intelligence/src/rag/mod.rs`                          | Public API: `generate_answer`, `RagAnswer`            |
| `crates/intelligence/src/rag/prompt.rs`                       | Sandwich ordering, type tags, token budget            |
| `crates/intelligence/src/rag/citation.rs`                     | Byte-walk citation parser                             |
| `crates/intelligence/src/lib.rs`                              | `pub mod rag` declaration (cfg-gated `embed`)         |
| `crates/intelligence/src/generate.rs`                         | `GenerateModelState`, `with_engine` (reused from v0.2)|
| `crates/executor/src/handlers/search.rs:170`                  | Insert point: `generate_rag_answer` shim call         |
| `crates/executor/src/handlers/search.rs:582`                  | cfg-gated shim + `RagAnswerStub` for non-embed builds |
| `crates/executor/src/handlers/search.rs:250`                  | Projection: `RagAnswer` → `AnswerResponse` + stats    |
| `crates/executor/src/output.rs`                               | `Output::SearchResults.answer` field                  |
| `crates/executor/src/types.rs`                                | `AnswerResponse` + 5 RAG fields on `SearchStatsOutput`|
| `crates/cli/src/format.rs:1090`                               | Human-mode rendering of the answer                    |
| `crates/engine/src/search/recipe.rs:48-62`                    | `Recipe.prompt`, `rag_context_hits`, `rag_max_tokens` |
| `crates/engine/src/search/recipe.rs:293-306`                  | `ModelsConfig.generate`                               |
| `crates/engine/src/search/recipe.rs:551-608`                  | `"rag"` builtin recipe                                |

---

## 14. Future work

Issues already filed and ordered for v0.7+:

| Issue   | Title                                       | Target  |
|---------|---------------------------------------------|---------|
| #2301   | Streaming RAG answers                       | v0.7    |
| #2303   | RAG answer caching                          | v0.7    |
| #2304   | Adaptive RAG routing                        | v0.7    |
| #2305   | Per-response quality signals                | v0.7+   |
| #2306   | Prompt injection defense                    | v0.7+   |
| —       | Multi-model routing (rules, fallback chains, cost-aware) | v0.8 |
| —       | Custom prompt experimentation via `db.experiment()`     | v0.7+ |
| —       | Temporal RAG (`prompt` + `diff` for change explanations)| v0.7+ |
| —       | Conversation / multi-turn context                       | future|

Each is structurally additive: it extends the recipe schema, the wire
types, or the trigger-gate logic, without disturbing the v0.6 single-call
flow described above.
