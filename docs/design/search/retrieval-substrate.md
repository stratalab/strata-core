# Unified Retrieval Substrate

## 1. One Operation: Retrieve

A database is a box of data. Users ask for things from the box. Sometimes the ask is precise:

> "Give me all red cricket balls."

Sometimes it's fuzzy:

> "Round object I can throw and hit with a wooden stick."

Usually it's a mix:

> "Round objects under $20 that I can use in a game, sorted by popularity."

These are all the same operation: **retrieval**. The difference is the precision of the specification, not the nature of the request. A traditional database draws an artificial line — exact predicates go through the query engine, fuzzy intent goes through the search engine — and then struggles when users want both in the same request.

Strata has one retrieval substrate. A **recipe** specifies what to retrieve and how. Exact predicates, fuzzy ranking, filtering, grouping, aggregation — these are all operators in the same pipeline. A "query" is a recipe with only exact operators. A "search" is a recipe with fuzzy operators. A filtered search uses both. There is no separate code path.

---

## 2. Invariants

Non-negotiable. Every implementation decision must preserve these.

**INV-1: Deterministic.** Same recipe + same request + same snapshot = identical response. Always. Tie-breaking is deterministic (score desc, entity_ref asc). No randomized algorithms in the retrieval path. No ambient state affects results.

**INV-2: Declarative.** The recipe is the complete specification. No hidden config, no environment variables, no learned state modifies execution. Two users submitting the same recipe against the same snapshot get the same results.

**INV-3: Snapshot-isolated.** Every retrieval executes against a consistent MVCC snapshot. Background processes may be writing concurrently, but the retrieval sees a point-in-time view.

**INV-4: Budget-bounded.** Every retrieval respects its budget. If limits are hit, return the best results found so far. Partial results are always better than no results.

---

## 3. The Recipe

A recipe is a JSON document. **Presence of a key means enabled. Absence means disabled.** Any section can be omitted — defaults apply.

### 3.1 Schema

```json
{
  "version": 1,

  "retrieve": {
    "scan": {
      "sources": ["kv", "json", "event"],
      "spaces": ["*"],
      "predicates": [
        {"field": "category", "op": "eq", "value": "pharmacology"},
        {"field": "year", "op": "gte", "value": 2024}
      ],
      "logic": "and"
    },

    "bm25": {
      "sources": ["kv", "json", "event"],
      "spaces": ["*"],
      "k": 50,
      "k1": 0.9,
      "b": 0.4,
      "field_weights": {"title": 3.0, "body": 1.0},
      "stemmer": "porter",
      "stopwords": "lucene33",
      "phrase_boost": 2.0,
      "proximity_boost": 0.0
    },

    "vector": {
      "collections": ["_system_embed_kv", "_system_embed_json"],
      "k": 50,
      "ef_search": 100
    },

    "graph": {
      "graph": "medical_ontology",
      "strategy": "ppr",
      "k": 50,
      "damping": 0.5,
      "max_hops": 2,
      "max_neighbors_per_hop": 50
    }
  },

  "filter": {
    "predicates": [
      {"field": "year", "op": "gte", "value": 2020},
      {"field": "status", "op": "in", "value": ["published", "preprint"]}
    ],
    "logic": "and",
    "time_range": {"start": "2025-01-01T00:00:00Z", "end": null},
    "score_threshold": 0.1
  },

  "fusion": {
    "method": "rrf",
    "k": 60,
    "weights": {"bm25": 1.0, "vector": 1.0, "graph": 0.3, "scan": 1.0}
  },

  "transform": {
    "sort": [{"by": "score", "order": "desc"}],
    "group_by": {"field": "category", "top_k_per_group": 3},
    "aggregate": [
      {"name": "categories", "type": "count", "field": "category"},
      {"name": "year_range", "type": "stats", "field": "year"}
    ],
    "deduplicate": "entity_ref",
    "limit": 10,
    "offset": 0
  },

  "control": {
    "budget_ms": 5000,
    "snapshot_version": null,
    "include_metadata": true,
    "include_snippets": true,
    "include_stage_scores": false,
    "snippet_length": 200
  }
}
```

### 3.2 What each section does

**`retrieve`** — Which retrieval operators to run. Each produces a candidate list. They run in parallel.

| Operator | What it does | Produces |
|----------|-------------|----------|
| `scan` | Evaluate exact predicates against indexes | Unranked candidate set |
| `bm25` | Full-text keyword matching via inverted index | Ranked candidates with relevance scores |
| `vector` | Dense embedding similarity via HNSW | Ranked candidates with similarity scores |
| `graph` | Structural retrieval via graph traversal (PPR, neighborhood) | Ranked candidates with graph proximity scores |

If a key is absent, that operator doesn't run. An empty object (`"bm25": {}`) enables the operator with all defaults.

Scoping: `scan` and `bm25` accept `sources` (which primitives) and `spaces` (which spaces). `vector` accepts `collections` (which vector collections). `graph` accepts `graph` (which named graph). Defaults: all primitives, all spaces, all `_system_embed_*` collections.

**`filter`** — Post-retrieval predicate filtering. Applied after fusion. Removes candidates that don't match. Separate from `scan` because `scan` is a retrieval operator (produces candidates), while `filter` is a narrowing step (removes candidates).

**`fusion`** — Merges candidate lists from multiple retrieval operators into one ranked list. Only meaningful when multiple operators are enabled. Defaults to RRF with k=60 and equal weights.

**`transform`** — Post-processing on the result set: sorting, grouping, aggregation, deduplication, pagination.

**`control`** — Execution parameters: time budget, explicit snapshot version.

### 3.3 The pipeline

```
1. Retrieve (parallel)
   ├─ scan    → unranked candidates
   ├─ bm25   → ranked candidates
   ├─ vector  → ranked candidates
   └─ graph   → ranked candidates

2. Fuse → single ranked list

3. Filter → narrow by predicates

4. Transform → sort, group, aggregate, deduplicate, paginate

5. Return
```

Every step except "return" is optional. A scan-only recipe skips fusion. A BM25-only recipe has trivial fusion (one list, nothing to merge). The pipeline is the same; the recipe controls which steps run.

### 3.4 Defaults

When a section or field is omitted:

| Field | Default |
|-------|---------|
| `retrieve.*.sources` | All primitives (`["kv", "json", "event"]`) |
| `retrieve.*.spaces` | All spaces (`["*"]`) |
| `retrieve.bm25.k` | 50 |
| `retrieve.bm25.k1` | 0.9 |
| `retrieve.bm25.b` | 0.4 |
| `retrieve.bm25.field_weights` | `{"body": 1.0}` |
| `retrieve.bm25.stemmer` | `"porter"` (options: `"porter"`, `"snowball"`, `"none"`) |
| `retrieve.bm25.stopwords` | `"lucene33"` (options: `"lucene33"`, `"smart571"`, `"none"`) |
| `retrieve.bm25.phrase_boost` | 2.0 (multiplier for exact phrase matches; 0 = disabled) |
| `retrieve.bm25.proximity_boost` | 0.0 (boost for terms appearing near each other; 0 = disabled) |
| `retrieve.vector.collections` | All `_system_embed_*` collections |
| `retrieve.vector.k` | 50 |
| `retrieve.vector.ef_search` | 100 |
| `retrieve.graph.graph` | Required (no default — user must name their graph) |
| `retrieve.graph.strategy` | `"ppr"` |
| `retrieve.graph.k` | 50 |
| `retrieve.graph.damping` | 0.5 |
| `retrieve.graph.max_hops` | 2 |
| `retrieve.graph.max_neighbors_per_hop` | 50 |
| `retrieve.scan.logic` | `"and"` |
| `filter` | No filtering |
| `filter.logic` | `"and"` |
| `filter.score_threshold` | None (return all results regardless of score) |
| `fusion.method` | `"rrf"` |
| `fusion.k` | 60 |
| `fusion.weights` | Equal weight (1.0) for all enabled operators |
| `transform.sort` | `[{"by": "score", "order": "desc"}]` |
| `transform.deduplicate` | `"entity_ref"` |
| `transform.limit` | 10 |
| `transform.offset` | 0 |
| `control.budget_ms` | 5000 |
| `control.snapshot_version` | Latest |
| `control.include_metadata` | `true` |
| `control.include_snippets` | `true` |
| `control.include_stage_scores` | `false` |
| `control.snippet_length` | 200 |

### 3.5 Predicate operators

Used in both `scan` and `filter`:

| Operator | Meaning | Example |
|----------|---------|---------|
| `eq` | Equals | `{"field": "status", "op": "eq", "value": "active"}` |
| `neq` | Not equals | `{"field": "status", "op": "neq", "value": "deleted"}` |
| `gt`, `gte`, `lt`, `lte` | Comparisons | `{"field": "year", "op": "gte", "value": 2024}` |
| `in` | Set membership | `{"field": "category", "op": "in", "value": ["A", "B"]}` |
| `not_in` | Not in set | `{"field": "status", "op": "not_in", "value": ["deleted"]}` |
| `contains` | Array contains | `{"field": "tags", "op": "contains", "value": "rust"}` |
| `prefix` | String prefix | `{"field": "key", "op": "prefix", "value": "user:"}` |
| `exists` | Field exists | `{"field": "author", "op": "exists", "value": true}` |

Predicate logic: `"and"` (default) or `"or"`.

### 3.6 Aggregation types

| Type | Output |
|------|--------|
| `count` | Value frequencies: `{"A": 47, "B": 12}` |
| `sum`, `avg`, `min`, `max` | Single number |
| `stats` | `{"min": .., "max": .., "avg": .., "sum": .., "count": ..}` |
| `histogram` | `[{"bucket": 2020, "count": 89}, ...]` (requires `interval`) |

Aggregations run over the full result set after filtering but before limit/offset.

---

## 4. Recipe Examples

### 4.1 Pure query

"Get all active enterprise users, sorted by name."

```json
{
  "retrieve": {
    "scan": {
      "predicates": [
        {"field": "status", "op": "eq", "value": "active"},
        {"field": "plan", "op": "eq", "value": "enterprise"}
      ]
    }
  },
  "transform": {
    "sort": [{"by": "name", "order": "asc"}],
    "limit": 50
  }
}
```

### 4.2 Keyword search

"Find documents about metformin side effects."

```json
{
  "retrieve": {
    "bm25": {}
  }
}
```

Everything defaults. BM25 with standard parameters, top 10.

### 4.3 Hybrid search

"Find documents about metformin, using both keywords and semantic similarity."

```json
{
  "retrieve": {
    "bm25": {},
    "vector": {}
  }
}
```

BM25 + vector, default RRF fusion, top 10.

### 4.4 Filtered hybrid search

"Find documents about metformin in pharmacology, published after 2020."

```json
{
  "retrieve": {
    "bm25": {"k": 100},
    "vector": {"k": 100}
  },
  "filter": {
    "predicates": [
      {"field": "category", "op": "eq", "value": "pharmacology"},
      {"field": "year", "op": "gt", "value": 2020}
    ]
  }
}
```

### 4.5 Search with facets

"Search for drug interactions. Show me category distribution and year histogram."

```json
{
  "retrieve": {
    "bm25": {"k": 200},
    "vector": {"k": 200}
  },
  "transform": {
    "aggregate": [
      {"name": "categories", "type": "count", "field": "category"},
      {"name": "by_year", "type": "histogram", "field": "year", "interval": 1}
    ],
    "limit": 10
  }
}
```

### 4.6 Grouped results

"Find cancer treatment documents, top 3 per treatment type."

```json
{
  "retrieve": {
    "bm25": {"k": 200}
  },
  "transform": {
    "group_by": {"field": "treatment_type", "top_k_per_group": 3}
  }
}
```

### 4.7 Scan + search combined

"Among active enterprise users, find those most relevant to 'ML infrastructure', and show department distribution."

```json
{
  "retrieve": {
    "scan": {
      "predicates": [
        {"field": "status", "op": "eq", "value": "active"},
        {"field": "plan", "op": "eq", "value": "enterprise"}
      ]
    },
    "bm25": {"k": 100}
  },
  "transform": {
    "aggregate": [
      {"name": "departments", "type": "count", "field": "department"}
    ],
    "limit": 20
  }
}
```

### 4.8 Pure aggregation

"How many documents per category? What's the date range?"

```json
{
  "retrieve": {
    "scan": {"predicates": []}
  },
  "transform": {
    "aggregate": [
      {"name": "categories", "type": "count", "field": "category"},
      {"name": "dates", "type": "stats", "field": "created_at"}
    ],
    "limit": 0
  }
}
```

### 4.9 Scoped search

"Search only JSON documents in the medical space."

```json
{
  "retrieve": {
    "bm25": {"sources": ["json"], "spaces": ["medical"]},
    "vector": {}
  }
}
```

### 4.10 Graph-augmented search

"Find information about metformin, including related drugs and conditions from the knowledge graph."

```json
{
  "retrieve": {
    "bm25": {"k": 50},
    "vector": {},
    "graph": {
      "graph": "medical_ontology",
      "strategy": "ppr",
      "k": 50,
      "damping": 0.5
    }
  },
  "fusion": {
    "weights": {"bm25": 1.0, "vector": 1.0, "graph": 0.3}
  }
}
```

The graph is user-created and lives on the user's branch. The substrate matches query terms against node labels in the graph, runs PPR from matched nodes, and returns structurally related entities as candidates. No model needed — anchor node resolution is string matching.

### 4.11 Tuned recipe (AutoResearch output)

What an optimized recipe might look like after AutoResearch tunes it for a medical corpus:

```json
{
  "retrieve": {
    "bm25": {
      "k": 75,
      "k1": 1.2,
      "b": 0.62,
      "field_weights": {"title": 4.2, "body": 1.0}
    },
    "vector": {
      "k": 60,
      "ef_search": 150
    }
  },
  "fusion": {
    "k": 45,
    "weights": {"bm25": 0.8, "vector": 1.2}
  },
  "transform": {"limit": 10}
}
```

Every parameter is slightly different from defaults because AutoResearch discovered these values work better for this specific dataset.

---

## 5. The Request

```json
{
  "query": "side effects of metformin in elderly patients",
  "recipe": "medical_hybrid_v2",
  "mode": "rag",
  "k": 10
}
```

| Field | Purpose | Default |
|-------|---------|---------|
| `query` | Query text. Used by BM25 for scoring. Intelligence layer embeds it for vector search. | Required |
| `recipe` | Name of a stored recipe, inline recipe object, or `null` for stored default. | Stored default |
| `mode` | Output mode: `"standard"`, `"rag"`, `"aggregate"` | `"standard"` |
| `k` | Shorthand override for `transform.limit`. | Recipe default |

**Modes:**

| Mode | What populates in the response |
|------|-------------------------------|
| `standard` | `hits` |
| `rag` | `hits` + `answer` (intelligence layer generates answer from hits via Qwen3) |
| `aggregate` | `aggregations` (hits still available but typically `limit: 0`) |

The recipe controls *how* to retrieve. The mode controls *what you get back*. Same retrieval pipeline, different output shape.

**Recipe resolution:** If `recipe` is null, use the stored default recipe for the database. If a named recipe is provided, look it up on `_system_`. If an inline recipe object is provided, use it directly (for experimentation/AutoResearch).

---

## 6. The Response

The response has a **fixed structure**. Every field is always present. Unpopulated fields are `null` or empty. The consumer writes one parser and uses it for every mode and recipe.

```json
{
  "hits": [
    {
      "entity_ref": {"type": "json", "branch": "main", "space": "default", "key": "doc:123"},
      "score": 0.847,
      "rank": 1,
      "snippet": "Common side effects include nausea...",
      "metadata": {"category": "pharmacology", "year": 2024}
    }
  ],

  "answer": {
    "text": "Common side effects of metformin include nausea, diarrhea, and stomach pain.",
    "sources": [1, 3, 5]
  },

  "aggregations": {
    "categories": {"pharmacology": 47, "toxicology": 12}
  },

  "groups": null,

  "stats": {
    "snapshot_version": 42891,
    "recipe": "default",
    "mode": "rag",
    "elapsed_ms": 214.3,
    "stages": {
      "bm25": {"elapsed_ms": 3.1, "candidates": 50, "corpus_size": 127000},
      "vector": {"elapsed_ms": 8.3, "candidates": 50, "corpus_size": 52000},
      "graph": {"elapsed_ms": 4.1, "candidates": 30, "anchors_matched": 3},
      "fusion": {"elapsed_ms": 0.4, "candidates_in": 130, "candidates_out": 94},
      "filter": {"elapsed_ms": 0.1, "removed": 20},
      "transform": {"elapsed_ms": 0.2},
      "generation": {"elapsed_ms": 198.2, "tokens": 47}
    },
    "budget_exhausted": false
  }
}
```

| Field | Type | When populated |
|-------|------|---------------|
| `hits` | array | Always (empty if `limit: 0` or results grouped) |
| `answer` | object or null | When `mode: "rag"` and intelligence layer available |
| `diff` | object or null | When `diff` parameter provided (temporal search) |
| `aggregations` | object or null | When `transform.aggregate` in recipe |
| `groups` | array or null | When `transform.group_by` in recipe |
| `stats` | object | Always |

---

## 7. The Evaluate Primitive

`evaluate()` is a substrate operation. It runs a recipe against an eval set and returns metrics. Deterministic.

```json
// Request
{
  "recipe": "medical_hybrid_v2",
  "eval_set": [
    {"query": "side effects of metformin", "relevant": ["doc:123", "doc:456"]},
    {"query": "warfarin interactions", "relevant": ["doc:789"]}
  ],
  "metrics": ["ndcg@10", "recall@10", "mrr"]
}

// Response
{
  "aggregate": {"ndcg@10": 0.634, "recall@10": 0.75, "mrr": 0.67},
  "per_query": [
    {"query": "side effects of metformin", "ndcg@10": 0.85, "recall@10": 1.0},
    {"query": "warfarin interactions", "ndcg@10": 0.42, "recall@10": 0.5}
  ]
}
```

The user provides queries and expected results. The intelligence layer handles query embedding for vector search internally. The user never touches embeddings.

This is the foundation for AutoResearch. Submit recipe variants, evaluate each, compare, iterate.

---

## 8. Raw Data Statistics

The substrate exposes facts about the data. Not interpretations — just numbers. Agents and the intelligence layer read these to make decisions.

```json
{
  "corpus": {
    "kv": {"count": 127450, "avg_length_tokens": 342, "p95_length_tokens": 1847},
    "json": {"count": 53200, "avg_length_tokens": 89},
    "event": {"count": 892100, "event_types": 47}
  },
  "vocabulary": {"unique_terms": 184200, "hapax_ratio": 0.43},
  "vector": {
    "_system_embed_kv": {"count": 127450, "dimension": 384}
  },
  "indexes": {
    "category": {"type": "tag", "cardinality": 12},
    "year": {"type": "numeric", "min": 2018, "max": 2025}
  }
}
```

The `indexes` section tells agents which fields are indexed and their cardinality, so recipes can use efficient predicates.

---

## 9. The Boundary: What Needs a Model Lives Outside

The rule is simple: **if it needs a model, it's not in the substrate.**

The substrate is model-free. It executes recipes using indexes, scoring functions, and deterministic algorithms. Everything that requires loading a model — embedding, expansion, reranking — lives in the intelligence layer or the agent.

| What | Where | Why |
|------|-------|-----|
| BM25 scoring | Substrate | Deterministic algorithm |
| Vector search (HNSW) | Substrate | Deterministic algorithm, embeddings already stored |
| Graph traversal (PPR) | Substrate | Deterministic algorithm, graph already built by user |
| Scan / filter / aggregate | Substrate | Deterministic operations |
| Fusion (RRF) | Substrate | Deterministic algorithm |
| Query embedding | Intelligence layer | Needs a model |
| Query expansion | Intelligence layer | Needs a model |
| Reranking | Intelligence layer | Needs a model |
| Auto-embedding at write time | Intelligence layer | Needs a model |

The intelligence layer wraps the substrate:

```
1. Intelligence layer: embed query → [0.012, -0.034, ...]
2. Intelligence layer: (optionally) expand query
3. Substrate: execute recipe → candidates
4. Intelligence layer: (optionally) rerank candidates
```

From the user's perspective, this is one call:

```python
results = db.search("metformin side effects", recipe=recipe)
```

The intelligence layer handles embedding and optional expansion/reranking transparently. The substrate does the retrieval. The user sends a query string and gets results.

Note: graph retrieval does NOT involve the intelligence layer. The user builds the graph, the substrate matches query terms to node labels and traverses. No model needed.

---

## 10. User API

Two entry points, both producing the same substrate call:

```python
# Simple — BM25-only, default recipe
results = db.search("metformin side effects")

# Full control — any recipe
results = db.search("metformin side effects", recipe=recipe)
results = db.retrieve("metformin side effects", recipe="medical_hybrid_v2")
```

`db.search(query)` is sugar for `db.retrieve(query, recipe=None)` which uses the baseline BM25 recipe.

`db.retrieve(query, recipe)` accepts a recipe name (string) or inline recipe (dict/JSON). The intelligence layer handles query embedding for vector search and optional expansion/reranking. The user sends a query string and gets results.

---

## 11. Recipe Storage

Recipes are stored as JSON documents on the `_system_` branch under `search/recipes/`. They are:

- **Named** — each recipe has a unique name (e.g., `"default"`, `"medical_hybrid_v2"`)
- **Versioned** — stored via normal Strata writes, so MVCC version history applies
- **Portable** — a recipe is a JSON file that can be exported, shared, or committed to source control

---

## 12. Graph Anchor Node Resolution

When the graph operator is enabled, the substrate needs to determine which graph nodes correspond to the query. The recipe controls this via the `strategy` field:

**PPR strategy:**
1. Tokenize the query text using the same tokenizer config as BM25 (stemmer, stopwords)
2. Match tokens against node labels in the named graph (case-insensitive, stemmed match)
3. Matched nodes become PPR seed nodes
4. Run Personalized PageRank from seeds with configured damping
5. Score documents connected to high-PPR nodes

**Neighborhood strategy:**
1. Same token-to-node matching as PPR
2. BFS expansion from matched nodes up to `max_hops`
3. Score documents by hop distance (closer = higher score)

If no nodes match the query terms, the graph operator returns zero candidates. Fusion proceeds with the other operators only.

---

## 13. Extensibility

The recipe schema is versioned (`"version": 1`). New retrieval operators (e.g., sparse vectors) are added as new keys under `retrieve`. New transform operations are added under `transform`. New fusion methods are added under `fusion.method`.

Because presence means enabled and absence means disabled, adding new operators is backward compatible. Existing recipes continue to work unchanged. The version bumps only for breaking changes to existing fields.

---

## 14. Open Questions

### 14.1 Scan semantics: pre-filter or candidate source?

When `scan` and `bm25` are both enabled, there are two valid interpretations:

**Option A: Scan as pre-filter.** Scan constrains what BM25 sees. Only documents matching scan predicates are scored by BM25. This matches the intent of "among active enterprise users, find those most relevant to X."

```
scan predicates → candidate set
                      ↓
               bm25 scores only these
                      ↓
               vector scores only these
                      ↓
               fusion (no scan list in fusion)
```

**Option B: Scan as candidate source.** Scan produces its own candidate list. BM25 produces its own. Fusion merges both via RRF. Documents found by both scan and BM25 get boosted. This matches the intent of "find documents that either match these predicates OR are relevant to this query."

```
scan  → candidate list (flat score 1.0)
bm25  → candidate list (relevance scores)
vector → candidate list (similarity scores)
          ↓
     fusion (RRF across all three)
```

Both are useful. Option A is more intuitive for constrained search ("search within X"). Option B is more useful for exploratory search ("find things matching X or related to Y").

The recipe could support both via a `scan.mode` field:
```json
"scan": {
  "mode": "prefilter",
  "predicates": [...]
}
```
vs
```json
"scan": {
  "mode": "candidate",
  "predicates": [...]
}
```

**Decision deferred.** Implement both. Let AutoResearch discover which mode works better for which use cases.

### 14.2 Predicate pushdown

When `filter` predicates match indexed fields, should the substrate automatically push them into retrieval for efficiency? The results are identical either way — it's a pure optimization. Yes, do this, but it's an implementation detail, not a schema decision.

### 14.3 Aggregation + grouping

Current schema supports `group_by` or `aggregate` independently. Should we support per-group aggregations (equivalent to SQL `GROUP BY x ... aggregate per group`)? Defer — implement both independently first, compose later if needed.
