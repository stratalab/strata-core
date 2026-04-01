# Named Recipes

**Status:** Proposed — supersedes the three-level merge from `retrieval-substrate.md` §3.4.

---

## 1. Problem

The three-level merge (builtin defaults → branch recipe → per-call override) has two fundamental problems:

1. **You can't turn things off.** If the builtin enables expansion, no overlay can disable it. `None` means "inherit," not "remove." We tried `enabled: false` — that's a hack contradicting "presence = enabled, absence = disabled."

2. **You can't reason about what runs.** When a search fires expansion or graph traversal, which level added it? Three-level merge makes this opaque.

The deeper issue: partial recipes that inherit from hidden defaults are the wrong abstraction. A recipe should be the complete, self-contained specification of what happens when you search. What you see is what runs.

---

## 2. Design: Named Recipes

A **recipe** is a fully self-contained JSON document that specifies every aspect of a search pipeline. No defaults, no inheritance, no merge. If a section is present, it runs. If absent, it doesn't.

Recipes are **named** and **stored**. Strata ships with built-in recipes. Users create their own. A search references a recipe by name.

```python
db.search("metformin side effects", recipe="keyword")
db.search("metformin side effects", recipe="hybrid")
db.search("metformin side effects", recipe="medical_v2")

# Inline recipe for experimentation
db.search("metformin side effects", recipe={
    "retrieve": {"bm25": {"k": 50, "k1": 0.9, "b": 0.4}},
    "transform": {"limit": 10}
})

# Shorthand overrides patch the selected recipe
db.search("metformin side effects", recipe="keyword", k=5)
```

The `recipe` parameter accepts:
- **A string** → look up the named recipe on `_system_` branch
- **A dict/JSON object** → use it directly as a complete recipe
- **Omitted** → use the recipe named `"default"`

Shorthand parameters (`k`, `as_of`, `diff`) are convenience patches on a complete recipe — not partial specs merged with hidden defaults.

---

## 3. Built-in Recipes

Strata ships with complete recipes covering the common search patterns. Each recipe is the full truth — every section that runs is specified, every section that doesn't is absent.

### `keyword`

BM25 only. No models needed. Fastest. Good for structured data, exact matches, known vocabulary.

```json
{
    "version": 1,
    "retrieve": {
        "bm25": {
            "sources": ["kv", "json", "event"],
            "k": 50, "k1": 0.9, "b": 0.4,
            "stemmer": "porter", "stopwords": "lucene33",
            "phrase_boost": 2.0
        }
    },
    "fusion": {"method": "rrf", "k": 60},
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "control": {"budget_ms": 5000, "include_snippets": true, "snippet_length": 200}
}
```

### `semantic`

Vector only. Pure embedding similarity. Useful when the query is semantically rich but keyword-poor (e.g., "things similar to this paragraph"), or when vocabulary mismatch is the main challenge.

```json
{
    "version": 1,
    "retrieve": {
        "vector": {"k": 50, "ef_search": 100}
    },
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "models": {"embed": "local:miniLM"},
    "control": {"budget_ms": 5000, "include_snippets": true, "snippet_length": 200}
}
```

### `hybrid`

BM25 + vector, RRF fusion. No expansion or reranking. Good balance of quality and speed. The workhorse for most use cases.

```json
{
    "version": 1,
    "retrieve": {
        "bm25": {
            "sources": ["kv", "json", "event"],
            "k": 50, "k1": 0.9, "b": 0.4,
            "stemmer": "porter", "stopwords": "lucene33",
            "phrase_boost": 2.0
        },
        "vector": {"k": 50, "ef_search": 100}
    },
    "fusion": {"method": "rrf", "k": 60},
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "models": {"embed": "local:miniLM"},
    "control": {"budget_ms": 5000, "include_snippets": true, "snippet_length": 200}
}
```

### `default`

Full pipeline. BM25 + vector + query expansion (lex/vec/hyde) + cross-encoder reranking with position-aware blending. Highest quality, uses local models for all intelligence operations.

```json
{
    "version": 1,
    "retrieve": {
        "bm25": {
            "sources": ["kv", "json", "event"],
            "k": 50, "k1": 0.9, "b": 0.4,
            "stemmer": "porter", "stopwords": "lucene33",
            "phrase_boost": 2.0
        },
        "vector": {"k": 50, "ef_search": 100}
    },
    "expansion": {
        "strategy": "full",
        "strong_signal_threshold": 0.85,
        "strong_signal_gap": 0.15,
        "min_shared_stems": 2,
        "original_weight": 2.0
    },
    "fusion": {"method": "rrf", "k": 60},
    "rerank": {
        "top_n": 20,
        "blending": {"rank_1_3": 0.75, "rank_4_10": 0.60, "rank_11_plus": 0.40}
    },
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "models": {
        "embed": "local:miniLM",
        "expand": "local:qwen3:1.7b",
        "rerank": "local:jina-reranker-v1-tiny"
    },
    "control": {"budget_ms": 5000, "include_snippets": true, "snippet_length": 200}
}
```

### `graph`

BM25 + graph traversal (Personalized PageRank). For datasets with a knowledge graph where structural relationships matter. Graph anchor resolution uses BM25 over indexed node text — no model needed.

```json
{
    "version": 1,
    "retrieve": {
        "bm25": {
            "sources": ["kv", "json", "event"],
            "k": 50, "k1": 0.9, "b": 0.4,
            "stemmer": "porter", "stopwords": "lucene33"
        },
        "graph": {
            "strategy": "ppr", "k": 50,
            "damping": 0.5, "max_hops": 2, "max_neighbors_per_hop": 50,
            "anchor_k": 10
        }
    },
    "fusion": {"method": "rrf", "k": 60, "weights": {"bm25": 1.0, "graph": 0.3}},
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "control": {"budget_ms": 5000, "include_snippets": true, "snippet_length": 200}
}
```

### `rag`

Full pipeline + RAG generation. Same retrieval as `default`, plus answer generation from top hits with citations. Uses local models for retrieval intelligence, cloud model for answer quality.

```json
{
    "version": 1,
    "retrieve": {
        "bm25": {
            "sources": ["kv", "json", "event"],
            "k": 50, "k1": 0.9, "b": 0.4,
            "stemmer": "porter", "stopwords": "lucene33",
            "phrase_boost": 2.0
        },
        "vector": {"k": 50, "ef_search": 100}
    },
    "expansion": {
        "strategy": "full",
        "strong_signal_threshold": 0.85,
        "strong_signal_gap": 0.15,
        "min_shared_stems": 2,
        "original_weight": 2.0
    },
    "fusion": {"method": "rrf", "k": 60},
    "rerank": {
        "top_n": 20,
        "blending": {"rank_1_3": 0.75, "rank_4_10": 0.60, "rank_11_plus": 0.40}
    },
    "transform": {"sort": [{"by": "score", "order": "desc"}], "deduplicate": "entity_ref", "limit": 10},
    "prompt": "Answer using only the provided context. Cite sources with [N].",
    "rag_context_hits": 5,
    "rag_max_tokens": 500,
    "models": {
        "embed": "local:miniLM",
        "expand": "local:qwen3:1.7b",
        "rerank": "local:jina-reranker-v1-tiny",
        "rag": "anthropic:claude-sonnet-4-6"
    },
    "control": {"budget_ms": 10000, "include_snippets": true, "snippet_length": 200}
}
```

---

## 4. The Full Pipeline

Every recipe can use any combination of these capabilities. The pipeline is:

```
1. Expand (if expansion section present)
   └─ Generate lex/vec/hyde variants via grammar-constrained LLM
   └─ Skip if BM25 probe detects strong signal
   └─ Hallucination guard: discard variants with low stem overlap

2. Retrieve (parallel, any combination)
   ├─ scan    → exact predicate matching against indexes → unranked candidates
   ├─ bm25   → full-text keyword matching via inverted index → ranked candidates
   ├─ vector  → dense embedding similarity via HNSW → ranked candidates
   └─ graph   → Personalized PageRank from BM25-resolved anchor nodes → ranked candidates

3. Fuse → merge candidate lists via weighted RRF → single ranked list

4. Filter → post-retrieval predicate filtering, time range, score threshold

5. Rerank (if rerank section present)
   └─ Cross-encoder re-scoring of top-N
   └─ Position-aware blending (protects high-confidence retrieval results)

6. Transform → sort, group, aggregate, deduplicate, paginate

7. Generate (if prompt section present and mode="rag")
   └─ Feed top hits to RAG model → grounded answer with citations

8. Enrich (if version section present)
   └─ Attach version history per hit via getv()

9. Return
```

Every step except "return" is controlled by the recipe. Present = runs. Absent = skipped.

---

## 5. User-Defined Recipes

Users create, store, and manage recipes:

```python
# Create from scratch
db.set_recipe("medical", {
    "version": 1,
    "retrieve": {
        "bm25": {"k": 100, "k1": 1.2, "b": 0.62, "stemmer": "porter"},
        "vector": {"k": 80, "ef_search": 150}
    },
    "filter": {
        "predicates": [{"field": "category", "op": "eq", "value": "pharmacology"}],
        "score_threshold": 0.1
    },
    "fusion": {"method": "rrf", "k": 45, "weights": {"bm25": 0.8, "vector": 1.2}},
    "rerank": {"top_n": 30, "blending": {"rank_1_3": 0.8, "rank_4_10": 0.5, "rank_11_plus": 0.3}},
    "transform": {"limit": 10},
    "models": {"embed": "openai:text-embedding-3-small", "rerank": "local:jina-reranker-v1-tiny"},
    "control": {"budget_ms": 5000}
})

# Use it
db.search("metformin dosage", recipe="medical")

# List / inspect
db.list_recipes()   # → ["default", "keyword", "hybrid", "semantic", "graph", "rag", "medical"]
db.get_recipe("medical")  # → full JSON
```

### Generate Recipe

`db.generate_recipe(description)` uses the local generation model to produce a complete recipe from natural language. This is an **authoring tool** — it runs before search, not during. The user inspects and stores the result.

```python
recipe = db.generate_recipe(
    "BM25 with high precision stemming, vector search using OpenAI embeddings, "
    "rerank top 30 with aggressive blending, filter to pharmacology category, "
    "return 20 results with 10 second budget"
)
print(recipe)                          # inspect
db.set_recipe("pharma_v1", recipe)     # store
db.search("drug interactions", recipe="pharma_v1")  # use
```

The generation model knows the full recipe schema — all retrieval operators (scan, bm25, vector, graph), expansion strategies (lex/vec/hyde), fusion methods, rerank blending, filter predicates, transform operations, RAG configuration, model routing, and control parameters.

### Storage

Recipes live in `_system_` space on the current branch. They fork with the branch via COW — fork a branch, all recipes come along. Override on the fork, parent is unaffected.

Built-in recipes are not stored — they're hardcoded and always available. User recipes with the same name shadow the built-in.

---

## 6. Experiments Use Named Recipes

`db.experiment()` compares named or inline recipes head-to-head:

```python
result = db.experiment(
    recipes=["keyword", "hybrid", "default", "medical"],
    eval_set=eval_set,
    metric="ndcg@10"
)
# → {"keyword": 0.32, "hybrid": 0.45, "default": 0.51, "medical": 0.54}
```

Each variant is a complete recipe. No partial overrides, no hidden inheritance. What you name is what runs.

---

## 7. What Changes From the Current Design

| Before | After |
|---|---|
| `builtin_defaults()` hardcoded function | Named built-in recipes (`keyword`, `hybrid`, `semantic`, `default`, `graph`, `rag`) |
| `Recipe::merge()` three-level deep merge | Gone. Recipe is always complete and self-contained. |
| `Recipe::resolve(builtin, branch, per_call)` | `resolve_recipe(name_or_inline)` — look up or use directly |
| Per-call recipe is a partial overlay | Per-call recipe is either a name or a complete inline recipe |
| `k` shorthand patches transform.limit | Same — `k` is the one allowed convenience patch |
| "presence = enabled" with hidden defaults that prevent disabling | Same principle, no hidden defaults. Absence means off. Period. |

### What stays the same

- The recipe schema (all sections: retrieve, expansion, filter, fusion, rerank, transform, prompt, models, version, control)
- The pipeline (expand → retrieve → fuse → filter → rerank → transform → generate → enrich)
- All retrieval operators (scan, bm25, vector, graph)
- All intelligence operations (embedding, expansion with lex/vec/hyde, reranking with position-aware blending, RAG)
- Multi-model routing (different models per operation)
- Temporal features (as_of, diff, version history)
- Substrate invariants (deterministic, declarative, snapshot-isolated, budget-bounded)
- `_system_` space storage and branch-level COW
- Predicate operators (eq, neq, gt, gte, lt, lte, in, not_in, contains, prefix, exists)
- Aggregation types (count, sum, avg, min, max, stats, histogram)
- Graph anchor resolution (BM25 over node text → PPR seeds)

### What goes away

- `builtin_defaults()` function
- `Recipe::merge()` and `Recipe::resolve()`
- The `merge_option()` helper and all per-struct merge impls
- The concept of "partial recipes" that inherit missing fields
- The three-level merge section of `retrieval-substrate.md` (§3.4, §3.5)

---

## 8. Migration

1. If a branch has a stored recipe (from the old API), it becomes the recipe named `"default"` on that branch.
2. `db.set_recipe(recipe)` (no name) continues to work — it sets the `"default"` recipe.
3. `db.set_recipe("name", recipe)` stores a named recipe.
4. `db.search("query")` uses `"default"`. `db.search("query", recipe="keyword")` uses the named recipe.

Backward compatible. Existing code continues to work.

---

## 9. Implementation

### Phase 1: Named recipe store

- Change recipe store from single-recipe to named-recipe storage in `_system_` space
- Register built-in recipes as constants (not stored, always available)
- `get_recipe(name)` / `set_recipe(name, recipe)` / `list_recipes()`
- Search handler: resolve `recipe` parameter (string → lookup, dict → use directly, absent → "default")
- Delete `builtin_defaults()`, `Recipe::merge()`, `Recipe::resolve()`, all merge impls

### Phase 2: CLI and Python SDK

- `strata recipe list` — show all available recipes (already partially implemented)
- `strata recipe show <name>` — print recipe JSON
- `strata recipe set <name> <json>` — store a recipe
- `strata search --recipe keyword "query"`
- Python: `db.search("query", recipe="keyword")`

### Phase 3: Generate recipe (with v0.7 AutoResearch)

- `db.generate_recipe(description)` → complete recipe JSON
- Uses generation model with full schema knowledge
- Validates output against recipe schema before returning
