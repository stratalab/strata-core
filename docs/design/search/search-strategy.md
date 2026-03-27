# Search Strategy

The overall plan for search in Strata: philosophy, architecture, and execution roadmap.

For the retrieval substrate spec (recipe schema, pipeline, invariants), see [`retrieval-substrate.md`](retrieval-substrate.md).

---

## 1. Philosophy

Every database treats search as a fixed pipeline with a few knobs. The designer picks parameters from papers, ships defaults, and hopes they work for everyone. In the age of AI agents, this is backwards. The data should drive how search is configured, not the database designer.

Strata takes a different approach:

- **The retrieval substrate** is a fully deterministic engine that accepts a recipe and executes it. It has no opinions. It does not try to be smart. Same recipe + same data + same snapshot = identical results, every time.

- **The intelligence layer** provides optional convenience tools powered by a local LLM (Qwen3): query embedding, expansion, reranking, write-time enrichment, synthetic eval generation. Useful but removable.

- **The AI agent** (user, application, or LLM) makes all decisions. It writes recipes, evaluates them, and iterates. The database gives tools — it doesn't reason by itself.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────┐
│  AI Agent (user / application / LLM)                     │
│  Makes all decisions. Writes recipes. Evaluates results. │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Intelligence Layer (optional)                           │
│                                                          │
│  Write-time:  auto-embedding, tagging, summarization     │
│  Query-time:  query embedding, expansion, reranking      │
│  Tune-time:   data profiling, synthetic eval generation, │
│               recipe suggestion, AutoResearch            │
│                                                          │
│  Enrichments written to _system_ branch.                 │
│  Powered by local Qwen3 (strata-inference).              │
│  Everything here is optional.                            │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Retrieval Substrate (engine)                            │
│                                                          │
│  Accepts a recipe (JSON). Executes deterministically.    │
│  Retrieval operators: scan, bm25, vector, graph.         │
│  Post-processing: filter, fusion, transform.             │
│  Primitives: evaluate(), statistics().                   │
│                                                          │
│  Model-free. If it needs a model, it's not here.         │
└─────────────────────────────────────────────────────────┘
```

### What lives where

| Concern | Layer | Why |
|---------|-------|-----|
| Execute retrieval given a recipe | Substrate | Deterministic, model-free |
| Store and version recipes | Substrate (`_system_` branch) | Recipes are data |
| Expose raw data statistics | Substrate | Statistics are facts |
| `evaluate(recipe, eval_set)` | Substrate | Deterministic execution + metric computation |
| Embed queries for vector search | Intelligence layer | Needs a model |
| Expand queries | Intelligence layer | Needs a model |
| Rerank results | Intelligence layer | Needs a model |
| Auto-embed documents at write time | Intelligence layer | Needs a model |
| Generate synthetic eval pairs | Intelligence layer | Needs a model |
| Run AutoResearch | Intelligence layer | Orchestration + model |
| Decide what recipe to use | Agent | Agent's decision |
| Build the knowledge graph | User | User's domain knowledge |

### Multi-model routing

The intelligence layer supports any GGUF model locally AND any OpenAI/Anthropic/Google-compatible API endpoint. The user assigns different models to different operations — cheap local models for high-volume operations (embedding, expansion), expensive cloud models where quality matters (RAG, AutoResearch). A single `db.search("query", mode="rag")` call may use three different models. See `intelligence-layer.md` section 2.

### The boundary

**If it needs a model, it's not in the substrate.**

The intelligence layer wraps the substrate:

```
1. Intelligence layer: embed query
2. Intelligence layer: (optionally) expand query
3. Substrate: execute recipe deterministically
4. Intelligence layer: (optionally) rerank results
```

The user sees one call: `db.search("query")`. The intelligence layer handles steps 1, 2, 4 transparently.

---

## 3. The `_system_` Branch

Strata uses `_system_` for derived/enriched data and search configuration. User data stays on user branches.

```
User branch:
  KV, JSON, events              (user's raw data)
  Graphs                        (user-built knowledge graphs with user-defined ontology)
  Vector collections             (user-created embeddings, if any)

_system_ branch:
  embeddings/                   shadow vector collections (auto-embed)
    _system_embed_kv
    _system_embed_json
    _system_embed_event
  enrichments/                  write-time derived data
    tags, summaries
  search/
    recipes/                    named recipe configurations
    eval_sets/                  evaluation pairs
    experiments/                AutoResearch history
  statistics/                   raw data statistics
```

Key design decisions:

- **Graphs live on user branches**, not `_system_`. The user defines the ontology and curates entities. One bad relationship screws up accuracy. The database doesn't auto-build graphs.
- **Embeddings live on `_system_`**. They're automatically derived by the intelligence layer. The user doesn't manage them.
- **Recipes live on `_system_`**. They're versioned, portable, and branchable.

---

## 4. The Intelligence Layer

Three features, two knobs. Detailed design in [`intelligence-layer.md`](intelligence-layer.md).

### 4.1 Features (public API)

| Feature | API | What it does |
|---------|-----|-------------|
| **RAG** | `db.search("query", mode="rag")` | Grounded answer with citations from retrieved hits |
| **Temporal Search** | `db.search("query", as_of=..., diff=...)` | Search at any point in time. Diff results across time. Explain what changed. |
| **AutoResearch** | `db.optimize_search(eval_set)` | Branch-parallel recipe optimization against user-provided eval pairs |

### 4.2 Knobs (configuration)

| Knob | What it does |
|------|-------------|
| **Query expansion** | Generates query variants before substrate call. Improves recall. |
| **Reranking** | Re-scores top-N results after substrate call. Improves precision. |

Configured via `db.configure(expansion=True, reranking=True)`. No new API — `db.search()` just returns better results.

### 4.3 Write-time enrichment

Auto-embedding exists today. Auto-tagging and summarization are future.

### 4.4 The intelligence layer is optional

- Without `embed` feature: substrate only. BM25 works. No RAG, no AutoResearch.
- With `embed` feature: all three features and both knobs available.
- Users who provide their own embeddings or write recipes by hand never need the intelligence layer.

### 4.5 Response format

Fixed structure. Six fields. Always present. Populated or null.

```json
{"hits": [], "answer": null, "diff": null, "aggregations": null, "groups": null, "stats": {}}
```

One parser for every mode, recipe, and feature combination.

---

## 5. Ablation Study

Validates that each layer of the pipeline contributes measurable improvement.

### 5.1 Levels

| Level | Recipe | What it measures |
|-------|--------|-----------------|
| **L0** | BM25 only | Baseline keyword retrieval |
| **L1** | BM25 + Vector (RRF) | Value of semantic similarity |
| **L2** | BM25 + Vector + Graph (RRF) | Value of ontology-guided structural signal |
| **L3** | L2 + LLM expansion + LLM reranking | Value of LLM intelligence |
| **L4** | AutoResearch-optimized L3 | Headroom from automated tuning |

### 5.2 Level recipes

**L0:**
```json
{"retrieve": {"bm25": {"k": 100}}}
```

**L1:**
```json
{"retrieve": {"bm25": {"k": 50}, "vector": {"k": 50}}}
```

**L2:**
```json
{
  "retrieve": {
    "bm25": {"k": 50},
    "vector": {"k": 50},
    "graph": {"graph": "domain_ontology", "strategy": "ppr", "k": 50, "damping": 0.5}
  },
  "fusion": {"weights": {"bm25": 1.0, "vector": 1.0, "graph": 0.3}}
}
```

**L3:** Same recipe as L2. Expansion and reranking happen in the intelligence layer (outside the recipe).

**L4:** AutoResearch-optimized recipe. All substrate parameters tuned via branch-parallel experimentation.

### 5.3 Metrics

| Metric | What it measures |
|--------|-----------------|
| NDCG@10 | Rank-aware relevance (primary) |
| Recall@10, Recall@100 | Coverage |
| MRR | First relevant result position |
| MAP | Precision across recall levels |
| Latency p50/p95/p99 | Performance cost |
| Tokens consumed | LLM cost (L3, L4) |

### 5.4 Datasets

| Dataset | What it tests |
|---------|--------------|
| MS MARCO | Large-scale passage retrieval |
| Natural Questions | Factoid questions |
| TREC-COVID | Domain-specific, keyword-heavy |
| FiQA | Semantic understanding |
| SciFact | Precision-critical |
| NFCorpus | Dense terminology |
| HotpotQA | Multi-hop (tests graph value) |
| MuSiQue | Multi-hop (tests graph value) |

### 5.5 Hypothesis

Ontology-guided retrieval (L2) will provide the most differentiated improvement on multi-hop and relational queries (HotpotQA, MuSiQue). BM25 finds documents containing query words. Vector finds semantically similar documents. The graph finds structurally related documents — a signal that's orthogonal to both text and embedding similarity.

---

## 6. Implementation Phases

### Phase 1: Substrate foundation

Build the recipe execution engine.

1. Define `Recipe` struct (Rust, serde-deserializable from JSON)
2. Implement recipe-driven retrieval: scan, bm25, vector operators
3. Implement filter, fusion (RRF), transform (sort, group, aggregate, limit)
4. Implement `evaluate()` and raw statistics
5. Fix primitive stubs (JSON, Event search return empty — #1936, #1949)
6. Store/retrieve recipes on `_system_` branch

**Success:** Existing search tests pass through the recipe path. `db.search("query")` works.

### Phase 2: Graph retrieval

Add graph as a retrieval operator.

1. Implement graph anchor node resolution (query terms → node label matching)
2. Implement PPR retrieval strategy
3. Wire graph into the recipe execution path
4. Validate on a graph-structured dataset

**Success:** L2 recipe returns graph-sourced hits alongside BM25 and vector hits.

### Phase 3: Intelligence layer

Build query-time and write-time tools.

1. Generalize auto-embed into pluggable enrichment pipeline
2. Add query embedding (already partially exists)
3. Add expansion helper (already partially exists)
4. Add reranking helper (already partially exists)
5. Add synthetic eval generation
6. Add data profiling

**Success:** Intelligence layer embeds queries transparently, generates eval pairs from data.

### Phase 4: AutoResearch

Build the optimization loop.

1. Branch-parallel experiment runner
2. Recipe mutation strategies
3. Qwen3 for experiment planning
4. Convergence detection
5. Wire `evaluate()` in the loop

**Success:** AutoResearch produces a recipe that measurably outperforms defaults.

### Phase 5: Ablation study

Run the study and produce results.

1. Prepare BEIR datasets in Strata
2. Run L0–L4 against all datasets
3. Produce result table
4. Compare against published baselines (Pyserini, ColBERTv2, ELSER)

**Success:** Publishable result table with consistent improvement across levels.

---

## 7. What's Innovative

The individual retrieval techniques are borrowed — BM25, HNSW, RRF, PPR are all published algorithms. The innovation is in the system design:

1. **O(1) branching makes search optimization cheap enough to automate.** 7,000 experiments overnight because branching costs microseconds. Architecturally impossible in any other search system.

2. **Deterministic, declarative recipes as the complete search specification.** The recipe is the full contract. Agents can iterate on it programmatically.

3. **Unified retrieval** — query and search are the same operation. Exact predicates, fuzzy ranking, filtering, aggregation all flow through one pipeline.

4. **Multi-model routing** — assign different models (local GGUF or cloud API) to different operations. Cheap fast models for embedding and expansion, expensive smart models for RAG and planning. A single search call uses three models. No other database has built-in model routing.

5. **In-process LLM** changes the economics of write-time enrichment and query-time intelligence. Zero network overhead, zero API cost for local operations.

The strongest claim: **branch-parallel experimentation is an architectural capability that requires copy-on-write storage. No existing search system has this.**

---

## 8. References

### Internal

- [`retrieval-substrate.md`](retrieval-substrate.md) — Recipe schema, pipeline, invariants
- [`../search-pipeline-implementation-plan.md`](../search-pipeline-implementation-plan.md) — Phase-by-phase search quality improvements
- [`../autoresearch-search-optimization.md`](../autoresearch-search-optimization.md) — Branch-parallel experimentation methodology

### Issues

- #1269 — Hybrid search pipeline
- #1270 — Graph-augmented search RFC
- #1322 — Query safety and score validation
- #1484 — Search pipeline implementation plan
- #1583 — BEIR validation suite
- #1640 — HippoRAG PPR
- #1872–#1876 — Primitive Searchable implementations
- #2106 — Search gaps inventory
- #2107 — Ablation study design

### External

- [BEIR](https://github.com/beir-cellar/beir) — Thakur et al., NeurIPS 2021
- [HippoRAG](https://arxiv.org/abs/2405.14831) — Gutierrez et al., NeurIPS 2024
- [RRF](https://dl.acm.org/doi/10.1145/1571941.1572114) — Cormack, Clarke, Buettcher, 2009
- [Autoresearch](https://github.com/karpathy/autoresearch) — Karpathy, 2025
