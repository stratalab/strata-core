# Intelligence Layer

The optional layer between the user and the retrieval substrate. Powered by strata-inference, which supports any GGUF model locally AND any OpenAI/Anthropic/Google-compatible API endpoint. The user assigns different models to different operations — cheap and fast where volume is high, expensive and smart where quality matters.

For the retrieval substrate spec, see [`retrieval-substrate.md`](retrieval-substrate.md).
For the overall strategy, see [`search-strategy.md`](search-strategy.md).

---

## 1. Three Features, Two Knobs

| | Name | What it does |
|---|------|-------------|
| **Feature** | RAG | Ask a question, get a grounded answer with citations |
| **Feature** | Temporal Search | Search at any point in time. Diff results across time. Explain what changed. |
| **Feature** | AutoResearch | The database tunes its own search recipe to your dataset |
| **Knob** | Query expansion | Generates query variants before retrieval. Improves recall. |
| **Knob** | Reranking | Re-scores top-N results after retrieval. Improves precision. |

Features are public API. Knobs are configuration — turn them on, search gets better, no new API surface.

---

## 2. Multi-Model Routing

This is the biggest unlock.

Every intelligence layer operation has a different cost/quality/latency profile. Embedding needs to be fast and run on every query. RAG generation needs to be smart and produce high-quality answers. AutoResearch planning needs to be smart but only runs occasionally. There is no single model that's optimal for all of these.

Most RAG frameworks force you to pick one model. Strata lets you assign a different model to each operation:

```python
db.configure(
    models={
        "embed":        "local:miniLM",
        "expansion":    "local:qwen3:1.7b",
        "rerank":       "local:qwen3:1.7b",
        "rag":          "anthropic:claude-sonnet-4-6",
        "autoresearch": "anthropic:claude-haiku-4-5",
    }
)
```

### 2.1 Why this matters

| Operation | Volume | Latency requirement | Quality requirement | Best fit |
|-----------|--------|--------------------|--------------------|----------|
| Embedding | Every query + every document | <10ms | Moderate | Local small model (MiniLM, nomic) |
| Expansion | Every query (if enabled) | <100ms | Moderate | Local small LLM (Qwen3 1.7B) |
| Reranking | Every query (if enabled) | <200ms | Moderate-high | Local cross-encoder or small LLM |
| RAG generation | Per query (when mode="rag") | <2s | High | Cloud model (Claude, GPT-4) |
| Temporal RAG | Per temporal query | <3s | High | Cloud model |
| AutoResearch planning | Per experiment round | <5s | High | Cloud model (smart, not fast) |

A single `db.search("query", mode="rag")` call might use three different models:
1. Local MiniLM embeds the query (5ms, free)
2. Local Qwen3 expands the query (100ms, free)
3. Substrate executes the recipe (15ms, deterministic)
4. Local Qwen3 reranks top-20 (200ms, free)
5. Claude Sonnet generates the answer (1s, $0.003)

Total: ~1.3s, $0.003. The answer is Claude-quality. Everything else is free.

### 2.2 Model specification

Models are specified as `provider:model_name`:

| Provider | Format | Examples |
|----------|--------|---------|
| `local` | `local:model_name` | `local:miniLM`, `local:qwen3:1.7b`, `local:ms-marco-MiniLM` |
| `anthropic` | `anthropic:model_id` | `anthropic:claude-sonnet-4-6`, `anthropic:claude-haiku-4-5` |
| `openai` | `openai:model_id` | `openai:gpt-4o`, `openai:gpt-4o-mini` |
| `google` | `google:model_id` | `google:gemini-2.5-flash` |
| Any OpenAI-compatible | `endpoint:model_id` | `ollama:llama3.1`, `together:meta-llama/Llama-3-8b` |

Local models are GGUF files loaded via llama.cpp (strata-inference). Zero network overhead, zero API cost. Cloud models call the provider's API.

### 2.3 Defaults

If `models` is not configured, all operations use local models:

| Operation | Default model |
|-----------|--------------|
| `embed` | `local:miniLM` |
| `expansion` | `local:qwen3:1.7b` |
| `rerank` | `local:qwen3:1.7b` |
| `rag` | `local:qwen3:1.7b` |
| `autoresearch` | `local:qwen3:1.7b` |

Everything works out of the box with local models. Zero cost. The user upgrades specific operations to cloud models when they want better quality for that operation.

### 2.4 AutoResearch can tune model assignment

Model assignment is another parameter AutoResearch can optimize. Given eval pairs, it can discover:

- "Switching RAG from local Qwen3 to Claude improves answer quality by 40%"
- "Switching expansion from Qwen3 to GPT-4o-mini doesn't improve NDCG — not worth the cost"
- "Cross-encoder reranking outperforms LLM-as-judge reranking on this dataset"

The model assignment becomes part of the optimized recipe output.

---

## 3. RAG

### What the user sees

```python
results = db.search(
    "What are the side effects of metformin?",
    mode="rag"
)

# results.answer.text → "Common side effects of metformin include nausea,
#   diarrhea, and stomach pain [1][3]. In rare cases, lactic acidosis
#   may occur, particularly in elderly patients [5]."
# results.answer.sources → [1, 3, 5]
# results.hits → [...full ranked results...]
```

One call. Grounded answer with citations AND ranked hits.

### How it works

```
User: db.search("What are the side effects of metformin?", mode="rag")
  │
  ▼
1. EMBED query (embed model, ~5ms)
  │
  ▼
2. SUBSTRATE executes recipe (deterministic, ~15ms)
   Returns ranked hits with snippets.
  │
  ▼
3. GENERATE answer from top hits (rag model, ~200ms local / ~1s cloud)
   Prompt: system instructions + retrieved snippets + user question.
   Output: grounded answer with [N] citations.
  │
  ▼
Return: { hits, answer: {text, sources}, aggregations, groups, stats }
```

### Prompt design

```
System: Answer the question using ONLY the provided context.
Cite sources using [1], [2], etc. If the context doesn't contain
the answer, say so.

Context:
[1] {snippet from hit #1}
[2] {snippet from hit #2}
[3] {snippet from hit #3}
[4] {snippet from hit #4}
[5] {snippet from hit #5}

Question: {user's query}
```

### Key properties

- **Grounded** — answer comes from retrieved context, not model knowledge
- **Cited** — every claim references a source hit by index
- **Honest** — if the context doesn't answer the question, it says so
- **Model-flexible** — local model for cost-sensitive use, cloud model for quality-sensitive use. Same prompt, same format, different quality/cost.
- **Deterministic retrieval** — hits are always the same (substrate invariant). The generated answer may vary slightly (LLM is non-deterministic).

### Configuration

| Param | Default | What it controls |
|-------|---------|-----------------|
| `models.rag` | `local:qwen3:1.7b` | Model used for answer generation |
| `rag_context_hits` | 5 | How many hits to include in the prompt |
| `rag_max_tokens` | 500 | Maximum answer length |

### Graceful degradation

If the RAG model is unavailable or inference fails: `answer` is `null`, hits still returned.

### Future: RLM evolution

RAG is a single pass: retrieve → generate. This evolves naturally to iterative refinement: retrieve → generate → examine → refine → retrieve again. No architectural changes — the loop wraps around the same substrate call.

---

## 4. Temporal Search

### What the user sees

```python
# Search as of a specific point in time
results = db.search("metformin side effects", as_of="2025-01-01")

# Diff: what changed between two points in time
results = db.search("metformin side effects",
    diff=("2024-01-01", "2025-06-01"))

# Temporal RAG: explain what changed
results = db.search(
    "How has our understanding of metformin side effects evolved?",
    mode="rag",
    diff=("2024-01-01", "2025-06-01")
)
```

### How it works

**Point-in-time search (`as_of`):**
1. Resolve timestamp to MVCC snapshot version
2. Execute recipe at that snapshot (substrate, deterministic)
3. Return results as they would have appeared at that time

**Temporal diff (`diff`):**
1. Execute recipe at snapshot T1 → results_before
2. Execute recipe at snapshot T2 → results_after
3. Compute diff: new hits, removed hits, changed hits, stable hits
4. Return structured diff

**Temporal RAG (`diff` + `mode="rag"`):**
1. Compute diff (as above)
2. Construct prompt with diff context (new/removed/changed snippets)
3. RAG model generates explanation of what changed
4. Return: answer + diff + hits from both snapshots

### Response format

Same fixed structure. `diff` field populated when temporal:

```json
{
  "hits": [],
  "answer": {
    "text": "Since January 2024, two new studies on metformin side effects have been added [1][2]. A new finding on vitamin B12 deficiency [1] has entered the top results.",
    "sources": [1, 2]
  },
  "diff": {
    "before_snapshot": 38201,
    "after_snapshot": 42891,
    "new_hits": [{"entity_ref": {}, "score": 0.82, "rank": 2}],
    "removed_hits": [{"entity_ref": {}, "previous_score": 0.65, "previous_rank": 4}],
    "changed_hits": [{"entity_ref": {}, "score": 0.91, "previous_score": 0.78}]
  },
  "aggregations": null,
  "groups": null,
  "stats": {}
}
```

### Why only Strata can do this

1. **MVCC snapshots at every write** — search at any historical state
2. **Deterministic recipes** — same recipe at two snapshots produces a meaningful diff
3. **In-process or cloud LLM** — explains the diff in natural language

### Use cases

| Use case | Query |
|----------|-------|
| Compliance | "What did we know about this drug interaction on March 15th?" |
| Research | "How has our data about climate impacts evolved this year?" |
| Auditing | "When did this document first appear in search results?" |
| Debugging | "Why did search quality change after the last data load?" |
| Change monitoring | "What's new in our legal corpus since last review?" |

---

## 5. AutoResearch

### What the user sees

```python
eval_set = [
    {"query": "metformin side effects", "relevant": ["doc:123", "doc:456"]},
    {"query": "drug interactions warfarin", "relevant": ["doc:789"]},
    # ... 50-100 pairs
]

result = db.optimize_search(eval_set, budget=1000)

# result.baseline_ndcg → 0.42
# result.optimized_ndcg → 0.68
# result.experiments_run → 847
# result.winning_recipe → {...}  (automatically saved as default)
```

### How it works

```
1. User provides eval_set (query → relevant docs pairs)
2. Evaluate current recipe → baseline metrics
3. Loop:
   a. AutoResearch model proposes N recipe variants
   b. For each variant: fork branch → evaluate → record metrics
   c. If best variant > current best → adopt it
   d. If no improvement for 5 rounds → stop
4. Save winning recipe as database default
5. Return: baseline → optimized metrics, experiments run, winning recipe
```

### What gets tuned

Every substrate parameter plus model assignments:

| Parameter | Range |
|-----------|-------|
| `bm25.k1` | 0.5 — 2.0 |
| `bm25.b` | 0.1 — 1.0 |
| `bm25.field_weights.*` | 0.0 — 5.0 |
| `bm25.stemmer` | porter / snowball / none |
| `bm25.stopwords` | lucene33 / smart571 / none |
| `bm25.phrase_boost` | 0.0 — 5.0 |
| `bm25.proximity_boost` | 0.0 — 2.0 |
| `vector.k` | 10 — 200 |
| `vector.ef_search` | 50 — 500 |
| `graph.damping` | 0.1 — 0.9 |
| `graph.max_hops` | 1 — 5 |
| `fusion.k` | 10 — 200 |
| `fusion.weights.*` | 0.0 — 3.0 |
| `models.expansion` | local vs cloud options |
| `models.rerank` | local vs cloud options |
| `expansion on/off` | enabled / disabled |
| `reranking on/off` | enabled / disabled |

### Experiment planning

The AutoResearch model reads previous experiment results and plans the next batch:

- **Round 1:** Broad sweep — major parameters (k1, b, fusion weights)
- **Round 5:** Narrow in — parameters that showed sensitivity
- **Round 10:** Fine-tune — small adjustments around the current best
- **Ablation:** Periodically disable one component to measure its contribution

A smarter model (Claude Haiku/Sonnet) produces better experiment plans than a local 1.7B model. This is where the multi-model routing pays off — use a cheap local model for high-volume operations, a smart cloud model for the planning step that runs once per round.

### Why it requires user-provided eval pairs

The database doesn't know what "good results" means for your data. The user provides 50-100 (query, relevant_docs) pairs that define their quality standard. This is the only human input required.

### Graceful degradation

If AutoResearch model unavailable: fall back to grid search (no intelligent planning, still works). If `evaluate()` fails: abort, return best recipe found so far.

---

## 6. Search Quality Knobs

Not features — configuration. Turn them on, `db.search()` returns better results.

### 6.1 Query Expansion

Generates query variants before calling the substrate.

| Strategy | What it does | Routed to |
|----------|-------------|-----------|
| `lex` | Keyword synonyms ("metformin" → "glucophage") | BM25 |
| `vec` | Semantic variants ("side effects" → "adverse reactions") | Vector |
| `hyde` | Hypothetical document | Vector |

**Strong signal skip:** BM25 probe first. If confident match → skip expansion.

**Hallucination guard:** Discard variants sharing fewer than 2 stemmed terms with original query.

### 6.2 Reranking

Re-scores top-N results after the substrate returns them.

- **Cross-encoder:** Feed (query, passage) pairs to a cross-encoder model. Fast (~100ms).
- **LLM-as-judge:** Feed all candidates to the rerank model. Flexible (~200ms).

### 6.3 Configuration

```python
db.configure(
    expansion=True,
    expansion_strategy="lex",
    reranking=True,
    rerank_top_n=20,
)
```

AutoResearch can discover whether these knobs help on your specific dataset and which strategies work best.

---

## 7. What's NOT in v1

| Feature | Why not now |
|---------|-----------|
| Auto-tagging | Extra configuration surface. |
| Summarization | Not critical path. |
| Iterative refinement (RLM) | Single-pass RAG must work first. |
| Data profiling | Nice-to-have. Will be killer later. |
| Recipe suggestion | User can start with defaults. |
| Synthetic eval generation | LLM-generated evals at 1.7B are too pattern-y. |

Upgrade path is additive. No substrate or response format changes needed.

---

## 8. Graceful Degradation

Every feature fails silently. The substrate always works.

| If this fails... | What happens |
|-----------------|-------------|
| Any model unavailable | That operation skipped. Results returned without it. |
| Cloud API timeout | Fall back to local model if configured, otherwise skip. |
| Embedding model unavailable | Vector search disabled. BM25 still works. |
| Everything fails | Substrate-only results. Same as if intelligence layer wasn't installed. |

---

## 9. Configuration

```python
db.configure(
    # Model routing — the key configuration
    models={
        "embed":        "local:miniLM",
        "expansion":    "local:qwen3:1.7b",
        "rerank":       "local:qwen3:1.7b",
        "rag":          "anthropic:claude-sonnet-4-6",
        "autoresearch": "anthropic:claude-haiku-4-5",
    },

    # Auto-embedding (exists today)
    auto_embed=True,

    # Search quality knobs
    expansion=False,
    expansion_strategy="lex",
    reranking=False,
    rerank_top_n=20,

    # RAG
    rag_context_hits=5,
    rag_max_tokens=500,
)
```

Feature gating:
- `cargo build` — substrate only. `db.search()` works. No intelligence layer.
- `cargo build --features embed` — intelligence layer with local models.
- `cargo build --features embed,anthropic` — add Anthropic cloud models.
- `cargo build --features embed,openai` — add OpenAI cloud models.
- `cargo build --features embed,google` — add Google cloud models.

---

## 10. API Summary

```python
# Search (always available)
results = db.search("query")
results = db.search("query", recipe="custom")
results = db.search("query", mode="rag")
results = db.search("query", as_of="2025-01-01")
results = db.search("query", diff=("2024-01-01", "2025-06-01"))
results = db.search("query", mode="rag", diff=("2024-01-01", "2025-06-01"))

# AutoResearch (requires eval pairs)
result = db.optimize_search(eval_set, budget=1000)

# Configuration
db.configure(models={...}, expansion=True, ...)
```

Three methods. That's it.

---

## 11. Response Format

Fixed structure. Six fields. Always present. Populated or null.

```json
{
  "hits": [],
  "answer": null,
  "diff": null,
  "aggregations": null,
  "groups": null,
  "stats": {}
}
```

One parser for every mode, every recipe, every feature combination.

---

## 12. References

- [`retrieval-substrate.md`](retrieval-substrate.md) — Recipe schema, pipeline, invariants
- [`search-strategy.md`](search-strategy.md) — Overall strategy, architecture, ablation study
- [`../autoresearch-search-optimization.md`](../autoresearch-search-optimization.md) — Detailed AutoResearch methodology
- #1633 — AutoResearch RFC
- #1644 — Bayesian optimization for search parameters
