# Intelligence Layer

The optional layer between the user and the retrieval substrate. Powered by a local LLM (Qwen3 via strata-inference). Three features, each building on the substrate.

For the retrieval substrate spec, see [`retrieval-substrate.md`](retrieval-substrate.md).
For the overall strategy, see [`search-strategy.md`](search-strategy.md).

---

## 1. Three Features

| Feature | What it does | Why it's unique |
|---------|-------------|-----------------|
| **RAG** | Ask a question, get a grounded answer with citations | Built into the database. One call. Zero infra. |
| **Temporal Search** | Search at any point in time. Diff results across time. Explain what changed. | Requires MVCC + deterministic search + in-process LLM. No other database has all three. |
| **AutoResearch** | The database tunes its own search recipe to your dataset | O(1) branching makes 1000s of experiments feasible. Architecturally impossible elsewhere. |

Everything else — query expansion, reranking, auto-tagging, summarization, iterative refinement, data profiling — is future work. Get these three right first.

---

## 2. RAG

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

One call. The user gets a grounded answer with source citations AND the ranked hits. No external APIs, no pipeline of tools, no infrastructure.

### How it works

```
User: db.search("What are the side effects of metformin?", mode="rag")
  │
  ▼
1. EMBED query (intelligence layer, ~5ms)
  │
  ▼
2. SUBSTRATE executes recipe (deterministic, ~15ms)
   Returns ranked hits with snippets.
  │
  ▼
3. GENERATE answer from top hits (Qwen3, ~200ms)
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

- **Grounded** — the answer comes from retrieved context, not model knowledge
- **Cited** — every claim references a source hit by index
- **Honest** — if the context doesn't answer the question, it says so
- **Deterministic retrieval** — the hits are always the same (substrate invariant). The generated answer may vary slightly (LLM is non-deterministic). This is acceptable — the user cares about answer quality, not byte-identical answers.

### Configuration

| Param | Default | What it controls |
|-------|---------|-----------------|
| `rag_context_hits` | 5 | How many hits to include in the prompt |
| `rag_max_tokens` | 500 | Maximum answer length |

### Graceful degradation

If Qwen3 is unavailable or inference fails: `answer` is `null`, hits are still returned. The user gets standard search results. RAG enhances but never blocks.

### Future: RLM evolution

RAG is a single pass: retrieve → generate. In the future, this evolves naturally to RLM Tier 1 (iterative refinement): retrieve → generate → examine → refine query → retrieve again. The substrate doesn't change. The intelligence layer just calls it in a loop. The upgrade path is:

```
RAG (v1):  retrieve once → generate
RLM (v2):  retrieve → examine → refine → retrieve → ... → generate
```

No architectural changes needed. The loop wraps around the same substrate call.

---

## 3. Temporal Search

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

```
1. Set snapshot_version to the MVCC version at the given timestamp
2. Execute recipe at that snapshot (substrate, deterministic)
3. Return results as they would have appeared at that time
```

This is straightforward — the substrate already supports `snapshot_version` in the recipe's control section. The intelligence layer just resolves a timestamp to a snapshot version.

**Temporal diff (`diff`):**

```
1. Execute recipe at snapshot T1 → results_before
2. Execute recipe at snapshot T2 → results_after
3. Compute diff:
   - new_hits: in results_after but not results_before
   - removed_hits: in results_before but not results_after
   - changed_hits: in both but score changed significantly
   - stable_hits: in both with similar scores
4. Return structured diff
```

**Temporal RAG (`diff` + `mode="rag"`):**

```
1. Compute diff (as above)
2. Construct prompt with diff context:
   "These results are NEW since {T1}: [snippets]"
   "These results were REMOVED since {T1}: [snippets]"
   "These results CHANGED in ranking: [snippets]"
3. Qwen3 generates explanation of what changed
4. Return: answer + diff + hits from both snapshots
```

### Response format

Same fixed structure, with temporal fields populated:

```json
{
  "hits": [/* results at T2 (latest snapshot) */],

  "answer": {
    "text": "Since January 2024, two new studies on metformin side effects have been added [1][2]. The GI-related side effects remain the most documented [3], but a new finding on vitamin B12 deficiency [1] has entered the top results.",
    "sources": [1, 2, 3]
  },

  "diff": {
    "before_snapshot": 38201,
    "after_snapshot": 42891,
    "new_hits": [
      {"entity_ref": {...}, "score": 0.82, "rank": 2}
    ],
    "removed_hits": [
      {"entity_ref": {...}, "previous_score": 0.65, "previous_rank": 4}
    ],
    "changed_hits": [
      {"entity_ref": {...}, "score": 0.91, "previous_score": 0.78, "rank": 1, "previous_rank": 3}
    ]
  },

  "aggregations": null,
  "groups": null,
  "stats": {/* includes timing for both snapshot queries */}
}
```

Wait — this adds a `diff` field to the response. That breaks the "fixed structure" rule.

**Resolution:** `diff` is always present in the response, `null` when not a temporal query. Same as `answer`, `aggregations`, `groups`. The fixed structure becomes:

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

Six fields. Always present. Populated or null.

### Why only Strata can do this

1. **MVCC snapshots at every write** — Strata can search at any historical state, not just "now"
2. **Deterministic recipes** — the same recipe at two snapshots produces a meaningful diff. If search were non-deterministic, the diff would be noise.
3. **In-process LLM** — Qwen3 explains the diff in natural language. Without it, the user gets raw before/after lists and has to interpret them.

No other database has all three. Elasticsearch doesn't snapshot. Pinecone doesn't do time-travel. Even databases with MVCC (Postgres) don't have search recipes or in-process LLMs.

### Use cases

| Use case | Query |
|----------|-------|
| Compliance | "What did we know about this drug interaction on March 15th?" |
| Research | "How has our data about climate impacts evolved this year?" |
| Auditing | "When did this document first appear in search results?" |
| Debugging | "Why did search quality change after the last data load?" |
| Change monitoring | "What's new in our legal corpus since last review?" |

---

## 4. AutoResearch

### What the user sees

```python
# User provides evaluation pairs (what good results look like)
eval_set = [
    {"query": "metformin side effects", "relevant": ["doc:123", "doc:456"]},
    {"query": "drug interactions warfarin", "relevant": ["doc:789"]},
    # ... 50-100 pairs
]

# One call. Database optimizes its own search.
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
   a. Qwen3 proposes N recipe variants (mutations of current best)
   b. For each variant:
      - Fork a branch
      - Store variant recipe
      - substrate.evaluate(variant, eval_set) → metrics
   c. If best variant > current best → adopt it
   d. If no improvement for 5 rounds → stop
4. Save winning recipe as database default
5. Return results: baseline → optimized metrics, experiments run, winning recipe
```

### What gets tuned

Every parameter in the recipe is a candidate:

| Parameter | Range | What it affects |
|-----------|-------|----------------|
| `bm25.k1` | 0.5 — 2.0 | Term frequency saturation |
| `bm25.b` | 0.1 — 1.0 | Document length normalization |
| `bm25.field_weights.*` | 0.0 — 5.0 | Per-field importance |
| `bm25.stemmer` | porter / snowball / none | Tokenization |
| `bm25.stopwords` | lucene33 / smart571 / none | Term filtering |
| `bm25.phrase_boost` | 0.0 — 5.0 | Exact phrase importance |
| `bm25.proximity_boost` | 0.0 — 2.0 | Term proximity importance |
| `vector.k` | 10 — 200 | Vector candidate count |
| `vector.ef_search` | 50 — 500 | HNSW recall vs speed |
| `graph.damping` | 0.1 — 0.9 | PPR exploration depth |
| `graph.max_hops` | 1 — 5 | Neighborhood traversal depth |
| `fusion.k` | 10 — 200 | RRF smoothing constant |
| `fusion.weights.*` | 0.0 — 3.0 | Per-source importance |

### Experiment planning (Qwen3's role)

Qwen3 doesn't do random search. It reads previous experiment results and plans the next batch intelligently:

- **Round 1:** Broad exploration — sweep major parameters (k1, b, fusion weights)
- **Round 5:** Narrow in — focus on parameters that showed sensitivity
- **Round 10:** Fine-tune — small adjustments around the current best
- **Ablation:** Periodically disable one component to measure its contribution

The prompt includes the experiment history and asks: "Given these results, what should we try next?"

### Why it requires user-provided eval pairs

The database doesn't know what "good results" means for your data. A medical corpus needs different relevance judgments than a legal corpus. The user provides 50-100 (query, relevant_docs) pairs that define their quality standard. This is the only human input required.

AutoResearch optimizes the recipe to maximize NDCG@10 (or another metric) on these eval pairs.

### Cost

- Each experiment: one `evaluate()` call (~100 queries × ~50ms = ~5 seconds)
- 1000 experiments: ~80 minutes sequential, faster with CPU parallelism
- Qwen3 planning: ~3 seconds per round, negligible vs experiment time

### Graceful degradation

If Qwen3 is unavailable: fall back to grid search over the parameter space (no intelligent planning, but still works). If evaluate() fails: abort and return the best recipe found so far.

---

## 5. Search Quality Knobs

Query expansion and reranking are not features — they're tunable configuration that makes search better. No new API. The user turns them on, `db.search()` returns better results.

### 5.1 Query Expansion

When enabled, the intelligence layer expands the query before calling the substrate.

| Strategy | What it does | Routed to |
|----------|-------------|-----------|
| `lex` | Keyword synonyms ("metformin" → "glucophage") | BM25 |
| `vec` | Semantic variants ("side effects" → "adverse reactions") | Vector |
| `hyde` | Hypothetical document | Vector |

**Strong signal skip:** BM25 probe first. If top score ≥ threshold with clear gap → skip expansion. Saves ~100ms on easy queries.

**Hallucination guard:** Discard variants sharing fewer than 2 stemmed terms with the original query.

### 5.2 Reranking

When enabled, the intelligence layer re-scores the top-N results after the substrate returns them.

- **Cross-encoder:** Feed (query, passage) pairs to a cross-encoder model. Deterministic, fast (~100ms for 20 candidates).
- **LLM-as-judge:** Feed all candidates to Qwen3, ask for relevance scores. Less calibrated but doesn't need a separate model (~200ms).

### 5.3 Configuration

```python
db.configure(
    expansion=True,           # on/off
    expansion_strategy="lex", # lex, vec, hyde, full
    reranking=True,           # on/off
    rerank_top_n=20,          # how many to rerank
)
```

These knobs are also tunable by AutoResearch — it can discover whether expansion helps on your specific dataset and which strategy works best.

---

## 6. What's NOT in v1

These come later, once the core features and knobs are solid:

| Feature | Why not now |
|---------|-----------|
| Auto-tagging | Requires user-defined tag categories. Extra configuration surface. |
| Summarization | Useful for long docs but not critical path. |
| Iterative refinement (RLM) | Natural evolution of RAG. Single-pass must work first. |
| Data profiling | Nice-to-have. Manual recipes work fine. |
| Recipe suggestion | User can start with defaults. |
| Synthetic eval generation | LLM-generated evals at 1.7B quality are too pattern-y to trust. |

The upgrade path is additive. Each feature wraps around the existing flow without changing the substrate or the response format.

---

## 7. Graceful Degradation

The intelligence layer never blocks search. Every feature fails silently.

| If this fails... | What happens |
|-----------------|-------------|
| Qwen3 unavailable | RAG returns `answer: null`. Temporal diff returns raw diff, no explanation. AutoResearch falls back to grid search. |
| Embedding model unavailable | Vector search disabled. BM25 still works. |
| Inference timeout | Feature skipped. Results returned without that enhancement. |
| Everything fails | You get substrate-only results. Same as if intelligence layer wasn't installed. |

**Principle:** The substrate always works. The intelligence layer enhances but never blocks.

---

## 8. Configuration

Minimal for v1.

```python
db.configure(
    # Auto-embedding (already exists, on by default)
    auto_embed=True,
    embed_model="miniLM",

    # Search quality knobs
    expansion=False,          # off by default
    expansion_strategy="lex",
    reranking=False,          # off by default
    rerank_top_n=20,

    # RAG
    rag_context_hits=5,       # hits included in RAG prompt
    rag_max_tokens=500,       # max answer length

    # AutoResearch — no config needed, just call db.optimize_search()
)
```

Feature gating:
- `cargo build` — substrate only. `db.search()` works. No RAG, no AutoResearch.
- `cargo build --features embed` — intelligence layer available. All three features work.

---

## 9. API Summary

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
```

Two methods. That's it.

---

## 10. Response Format

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

## 11. References

- [`retrieval-substrate.md`](retrieval-substrate.md) — Recipe schema, pipeline, invariants
- [`search-strategy.md`](search-strategy.md) — Overall strategy, architecture, ablation study
- [`../autoresearch-search-optimization.md`](../autoresearch-search-optimization.md) — Detailed AutoResearch methodology
- #1633 — AutoResearch RFC
- #1644 — Bayesian optimization for search parameters
