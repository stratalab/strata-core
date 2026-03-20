# RFC: Composable Search Substrate

**Status:** Proposal
**Date:** 2026-03-20
**Related Issues:** #1269 (Hybrid Search), #1270 (Graph-Augmented Search), #1581 (ACORN Filtered Search), #1587 (AI Paper — AutoResearch)

---

## Problem

Strata's search pipeline today is a fixed sequence: BM25 → vector → RRF fusion → optional expansion → optional reranking. The pipeline works, but every user gets the same recipe. An agent searching for "similar documents" needs a different pipeline than one searching for "what changed after the incident" or "entities related to Alice."

There is no way to:
- Skip BM25 when the query is purely semantic
- Add graph traversal as a retrieval signal
- Change fusion strategy per query
- Chain multiple expansion strategies
- Use different reranking models for different query types
- Let an agent learn which recipe works best for its workload

The current pipeline is a monolith. It needs to become a composable algebra.

---

## Design

### The Search Recipe

A search recipe is an ordered pipeline of operators. Each operator takes candidates in and produces candidates out. The recipe is the agent's retrieval strategy — declarative, composable, optimizable.

```rust
pub struct SearchRecipe {
    /// Human-readable name for this recipe
    pub name: Option<String>,
    /// Ordered pipeline of retrieval and scoring operators
    pub stages: Vec<SearchStage>,
    /// Final result limit
    pub limit: usize,
    /// Time budget for the entire pipeline
    pub budget_ms: u64,
}
```

### Operators

Each operator is a self-contained retrieval or scoring step:

```rust
pub enum SearchStage {
    /// Keyword retrieval via inverted index
    BM25 {
        /// Fields to search (default: all indexed fields)
        fields: Option<Vec<String>>,
        /// BM25 parameters (default: k1=0.9, b=0.4)
        k1: Option<f64>,
        b: Option<f64>,
        /// Max candidates to retrieve
        k: usize,
    },

    /// Vector similarity retrieval
    Vector {
        /// Collection to search (default: auto-embed shadow collection)
        collection: Option<String>,
        /// Distance metric override (default: collection's metric)
        metric: Option<DistanceMetric>,
        /// Max candidates to retrieve
        k: usize,
        /// Metadata filter (pre-filter)
        filter: Option<MetadataFilter>,
    },

    /// Graph traversal retrieval
    Graph {
        /// Starting entities (from previous stage results or explicit IDs)
        seed: GraphSeed,
        /// Traversal strategy
        strategy: GraphStrategy,
        /// Max traversal depth
        max_hops: usize,
        /// Edge type filter
        edge_types: Option<Vec<String>>,
    },

    /// Query expansion via LLM
    Expand {
        /// Expansion strategy
        strategy: ExpandStrategy,
        /// Model to use (default: configured model)
        model: Option<String>,
        /// Number of expansions to generate
        n: usize,
    },

    /// Fusion of multiple candidate lists
    Fuse {
        /// Fusion method
        method: FusionMethod,
    },

    /// Cross-encoder reranking
    Rerank {
        /// Model to use
        model: Option<String>,
        /// Max candidates to rerank (top-N from previous stage)
        top_n: usize,
        /// Blending weights with previous scores
        blend: BlendStrategy,
    },

    /// Score boost based on signals
    Boost {
        /// Boost signal
        signal: BoostSignal,
    },

    /// Filter candidates
    Filter {
        /// Filter predicate
        predicate: FilterPredicate,
    },
}

pub enum GraphSeed {
    /// Use entity references from previous stage results
    FromResults,
    /// Start from explicit entity IDs
    Entities(Vec<String>),
}

pub enum GraphStrategy {
    /// Breadth-first traversal, score by hop distance
    BFS,
    /// PageRank authority score
    PageRank { damping: f64, iterations: usize },
    /// Personalized PageRank from seed entities
    PersonalizedPageRank { damping: f64, iterations: usize },
}

pub enum ExpandStrategy {
    /// Keyword reformulations (BM25 only)
    Lexical,
    /// Semantic rephrasings (vector search)
    Semantic,
    /// Hypothetical document generation (HyDE)
    HyDE,
    /// All three
    Full,
}

pub enum FusionMethod {
    /// Reciprocal Rank Fusion
    RRF { k: usize },
    /// Weighted linear combination of scores
    Linear { weights: Vec<f64> },
    /// Learned fusion (future)
    Learned { model: String },
}

pub enum BlendStrategy {
    /// Replace previous scores entirely
    Replace,
    /// Weighted average with previous scores
    Weighted { rerank_weight: f64 },
    /// Position-aware blending (current default)
    PositionAware,
}

pub enum BoostSignal {
    /// Recency: boost newer entries
    Recency { decay: DecayFunction },
    /// Graph proximity: boost entries reachable from context entities
    GraphProximity { source_entities: Vec<String>, max_hops: usize },
    /// Access frequency: boost frequently accessed entries
    Frequency,
    /// Metadata field value: boost entries matching criteria
    Metadata { field: String, value: Value, weight: f64 },
}

pub enum DecayFunction {
    /// Linear decay from 1.0 to 0.0 over the window
    Linear { window_hours: f64 },
    /// Exponential decay with half-life
    Exponential { half_life_hours: f64 },
    /// Step function: full weight within window, zero outside
    Step { window_hours: f64 },
}

pub enum FilterPredicate {
    /// Filter by primitive type
    PrimitiveType(Vec<PrimitiveType>),
    /// Filter by space
    Space(Vec<String>),
    /// Filter by minimum score
    MinScore(f64),
    /// Filter by metadata field
    Metadata(MetadataFilter),
}
```

### Default Recipes

Pre-built recipes that work out of the box:

```rust
impl SearchRecipe {
    /// Fast keyword search. No LLM, no vectors.
    pub fn keyword(limit: usize) -> Self {
        SearchRecipe {
            name: Some("keyword".into()),
            stages: vec![
                SearchStage::BM25 { fields: None, k1: None, b: None, k: limit * 2 },
            ],
            limit,
            budget_ms: 100,
        }
    }

    /// Hybrid BM25 + vector with RRF fusion. The default.
    pub fn hybrid(limit: usize) -> Self {
        SearchRecipe {
            name: Some("hybrid".into()),
            stages: vec![
                SearchStage::BM25 { fields: None, k1: None, b: None, k: limit * 3 },
                SearchStage::Vector { collection: None, metric: None, k: limit * 3, filter: None },
                SearchStage::Fuse { method: FusionMethod::RRF { k: 60 } },
            ],
            limit,
            budget_ms: 200,
        }
    }

    /// Full pipeline: expand → BM25 + vector → fuse → graph boost → rerank.
    pub fn full(limit: usize) -> Self {
        SearchRecipe {
            name: Some("full".into()),
            stages: vec![
                SearchStage::Expand { strategy: ExpandStrategy::Full, model: None, n: 3 },
                SearchStage::BM25 { fields: None, k1: None, b: None, k: limit * 5 },
                SearchStage::Vector { collection: None, metric: None, k: limit * 5, filter: None },
                SearchStage::Fuse { method: FusionMethod::RRF { k: 60 } },
                SearchStage::Boost { signal: BoostSignal::Recency {
                    decay: DecayFunction::Exponential { half_life_hours: 168.0 }
                }},
                SearchStage::Rerank { model: None, top_n: 20, blend: BlendStrategy::PositionAware },
            ],
            limit,
            budget_ms: 3000,
        }
    }

    /// Graph-centric: start from entities, traverse, then enrich with text search.
    pub fn graph_centric(seed_entities: Vec<String>, limit: usize) -> Self {
        SearchRecipe {
            name: Some("graph".into()),
            stages: vec![
                SearchStage::Graph {
                    seed: GraphSeed::Entities(seed_entities),
                    strategy: GraphStrategy::PersonalizedPageRank { damping: 0.85, iterations: 20 },
                    max_hops: 3,
                    edge_types: None,
                },
                SearchStage::Vector { collection: None, metric: None, k: limit * 3, filter: None },
                SearchStage::Fuse { method: FusionMethod::RRF { k: 60 } },
            ],
            limit,
            budget_ms: 500,
        }
    }
}
```

---

## Execution Model

### Pipeline Execution

The recipe executor processes stages left to right. Each stage receives the accumulated candidate set and produces a new one:

```rust
pub fn execute_recipe(
    db: &Database,
    branch_id: &BranchId,
    query: &str,
    recipe: &SearchRecipe,
) -> Result<SearchResponse> {
    let mut candidates = CandidateSet::empty();
    let mut expanded_queries: Vec<ExpandedQuery> = vec![
        ExpandedQuery { text: query.to_string(), weight: 1.0, query_type: QueryType::Original }
    ];
    let deadline = Instant::now() + Duration::from_millis(recipe.budget_ms);

    for stage in &recipe.stages {
        if Instant::now() > deadline {
            break; // budget exhausted — return what we have
        }

        match stage {
            SearchStage::BM25 { .. } => {
                // Run BM25 for each query (original + expansions)
                // Each produces a ranked list
                // Add all to candidates with source tag
                for eq in &expanded_queries {
                    let results = bm25_search(db, branch_id, &eq.text, stage);
                    candidates.add_list(&eq, results);
                }
            }

            SearchStage::Vector { .. } => {
                // Embed each query, run vector search
                // Add results to candidates
                for eq in &expanded_queries {
                    let embedding = embed_query(db, &eq.text)?;
                    let results = vector_search(db, branch_id, &embedding, stage);
                    candidates.add_list(&eq, results);
                }
            }

            SearchStage::Graph { .. } => {
                // Traverse graph from seed entities
                // Produce candidates scored by traversal
                let graph_results = graph_traverse(db, branch_id, stage, &candidates);
                candidates.add_list(&expanded_queries[0], graph_results);
            }

            SearchStage::Expand { .. } => {
                // Generate additional query variants
                // These are used by subsequent BM25/Vector stages
                let expansions = expand_query(db, query, stage)?;
                expanded_queries.extend(expansions);
            }

            SearchStage::Fuse { method } => {
                // Fuse all candidate lists into a single ranked list
                candidates = fuse_candidates(&candidates, method);
            }

            SearchStage::Rerank { .. } => {
                // Rerank top candidates with cross-encoder
                candidates = rerank_candidates(db, query, &candidates, stage)?;
            }

            SearchStage::Boost { signal } => {
                // Apply score boost to existing candidates
                candidates = boost_candidates(db, branch_id, &candidates, signal)?;
            }

            SearchStage::Filter { predicate } => {
                // Remove candidates that don't match
                candidates = filter_candidates(&candidates, predicate);
            }
        }
    }

    Ok(candidates.into_response(recipe.limit))
}
```

### Budget Management

Each stage tracks its execution time. If the budget is exhausted mid-pipeline, the executor returns the best results available so far. This means:

- A full pipeline with a 100ms budget gracefully degrades to BM25-only
- An agent can set aggressive budgets for latency-sensitive queries and generous budgets for thorough research queries
- The budget is a contract: "give me the best results you can within this time"

---

## Agent API

### Python SDK

```python
# Default search — uses hybrid recipe
results = db.search("what happened yesterday?")

# Keyword only — fast, no embedding
results = db.search("error 500", recipe="keyword")

# Full pipeline — expansion, graph, reranking
results = db.search("security incidents related to Alice", recipe="full")

# Custom recipe — agent constructs its own
recipe = db.recipe("my_investigation") \
    .expand(strategy="hyde", n=2) \
    .bm25(k=50) \
    .vector(k=50) \
    .fuse(method="rrf", k=60) \
    .graph(seed="from_results", strategy="pagerank", hops=2) \
    .boost(signal="recency", half_life_hours=24) \
    .rerank(top_n=20) \
    .limit(10) \
    .budget_ms(2000)

results = db.search("what changed after the breach?", recipe=recipe)

# Save recipe for reuse
db.save_recipe("incident_investigation", recipe)

# Load saved recipe
results = db.search(query, recipe="incident_investigation")
```

### CLI

```bash
# Default
strata search "what happened yesterday?"

# With named recipe
strata search "security incidents" --recipe full

# Inline recipe construction
strata search "related entities" \
  --bm25 k=50 \
  --vector k=50 \
  --fuse rrf \
  --graph pagerank hops=2 \
  --rerank top_n=20 \
  --limit 10
```

### MCP Tool

```json
{
  "tool": "strata_search",
  "arguments": {
    "query": "what changed after the breach?",
    "recipe": {
      "stages": [
        {"type": "bm25", "k": 50},
        {"type": "vector", "k": 50},
        {"type": "fuse", "method": "rrf", "k": 60},
        {"type": "graph", "strategy": "pagerank", "hops": 2},
        {"type": "rerank", "top_n": 20}
      ],
      "limit": 10,
      "budget_ms": 2000
    }
  }
}
```

---

## Stored Recipes

Recipes can be saved as named configurations on the `_system_` branch:

```rust
/// A stored recipe, persisted as JSON on the _system_ branch.
/// Key: __recipe__{name}
pub struct StoredRecipe {
    pub name: String,
    pub recipe: SearchRecipe,
    pub created_at: u64,
    pub created_by: Option<String>,
    pub description: Option<String>,
}
```

Stored recipes are branchable — an agent can fork a branch, modify a recipe, test it, and merge back if it performs better. This is the foundation of AutoResearch (#1587): the optimizer creates recipe variants, evaluates them on branches, and promotes the winner.

---

## AutoResearch Integration

The composable search substrate is what makes AutoResearch (#1587) possible at the recipe level, not just the parameter level.

Today's AutoResearch optimizes parameters within a fixed pipeline (BM25 k1/b, RRF k, rerank weights). With composable recipes, AutoResearch can optimize the pipeline structure itself:

```
Experiment 1: BM25 → vector → RRF → rerank
Experiment 2: expand(hyde) → BM25 → vector → RRF → rerank
Experiment 3: BM25 → vector → RRF → graph(pagerank) → rerank
Experiment 4: expand(hyde) → BM25 → vector → graph(pagerank) → RRF → rerank
...
```

Each experiment is a different recipe on a different branch. The optimizer discovers not just the best parameters, but the best pipeline structure for the workload.

This is the retrieval equivalent of neural architecture search — except instead of searching over network architectures, we're searching over retrieval pipeline architectures.

---

## Relationship to Existing Search

### Migration Path

The current `HybridSearch` implementation becomes a recipe executor with three default recipes (keyword, hybrid, full). Existing behavior is preserved — `db.search(query)` uses the hybrid default exactly as it does today.

### What Changes

| Current | After |
|---------|-------|
| Fixed pipeline in `hybrid.rs` | Recipe executor with composable stages |
| Hardcoded BM25 + vector + RRF | Configurable operators |
| No graph integration in search | Graph traversal as a stage |
| No per-query pipeline control | Agent selects or constructs recipe |
| Parameter optimization only | Pipeline structure optimization |

### What Doesn't Change

- BM25 scoring implementation
- Vector search implementation
- RRF fusion implementation
- Query expansion implementation
- Reranking implementation
- Auto-embedding

All existing implementations become operators in the algebra. The substrate is the composition layer on top.

---

## Implementation Phases

### Phase 1: Recipe Data Model + Executor (~2 weeks)

- Define `SearchRecipe`, `SearchStage`, and all operator enums
- Implement recipe executor with stage-by-stage processing
- Implement budget management (deadline-based graceful degradation)
- Convert existing `HybridSearch` into recipe execution (default recipes: keyword, hybrid, full)
- All existing search tests pass unchanged

### Phase 2: Graph Operators (~1 week)

- Implement `SearchStage::Graph` with BFS and PageRank strategies
- Implement `GraphSeed::FromResults` (extract entities from previous stage)
- Implement `BoostSignal::GraphProximity`
- Wire graph operators into the recipe executor

### Phase 3: Agent API + Stored Recipes (~1 week)

- Python SDK recipe builder API
- CLI `--recipe` flag and inline stage construction
- MCP tool with recipe JSON
- Stored recipes on `_system_` branch (CRUD)
- Recipe branchability (fork recipe, modify, test)

### Phase 4: Advanced Operators (~2 weeks)

- `BoostSignal::Recency` with configurable decay functions
- `BoostSignal::Frequency` (access tracking)
- `FusionMethod::Linear` (weighted combination)
- `FilterPredicate` variants (type, space, score, metadata)
- `BlendStrategy` variants for reranking

### Phase 5: AutoResearch Integration (~1 week)

- Recipe variant generation (structural mutations)
- Branch-parallel recipe evaluation
- Recipe promotion (merge winning recipe to main)
- Recipe evolution tracking (which recipes were tried, which won)

---

## Open Questions

1. **Operator ordering constraints**: Should the executor enforce that Fuse comes after retrieval stages? Or allow arbitrary ordering and let agents experiment? Recommendation: allow arbitrary ordering. The executor handles any input — if Fuse is called with one list, it's a no-op. If Rerank is called with no candidates, it's a no-op. Agents learn what works.

2. **Cross-branch recipes**: Can a recipe reference data on a different branch? E.g., "search branch A, boost by graph on branch B." This enables interesting patterns (search production data, boost by experimental graph) but adds complexity. Recommendation: defer to Phase 5.

3. **Recipe serialization format**: JSON for storage and API, Rust structs for internal execution. **No custom DSL.** The method-chaining SDK API (Python/Node) and JSON format (CLI, MCP, strata.toml) are the two representations. Both are standard formats that every language and every coding agent already knows how to produce. A custom text DSL would add a parser, a grammar, error messages, and edge cases for marginal benefit over JSON. The host language *is* the DSL.

4. **Streaming results**: Should the executor stream results as stages complete, or batch? For interactive agents, streaming the first results from BM25 while vector search is still running could improve perceived latency. Recommendation: batch first. Streaming in a future phase.

5. **Recipe versioning**: When a recipe is updated, should old versions be preserved? This matters for reproducibility — "what recipe produced this result?" Recommendation: yes, recipes are versioned via MVCC like everything else.

---

## Success Criteria

1. `db.search(query)` continues to work exactly as before (backward compatible)
2. Agents can construct custom recipes that outperform the default on their specific workload
3. AutoResearch can discover optimal recipe structures through branch-parallel experimentation
4. Latency budget is respected — no query exceeds its budget regardless of recipe complexity
5. Graph traversal is a composable retrieval signal, not a separate API
