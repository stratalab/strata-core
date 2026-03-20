# RFC: AutoResearch — Self-Optimizing Search

**Status:** Proposal
**Date:** 2026-03-20
**Related:** #1632 (Composable Search Substrate), #1587 (AI Paper), `docs/design/autoresearch-search-optimization.md` (original internal design)

---

## The Idea

Strata is the only database that can optimize its own search quality — because it's the only database with O(1) branching.

The user provides evaluation pairs: `(query, expected_results)`. Strata forks hundreds of branches, tries different search recipes and parameters on each, evaluates against the pairs, and merges the winning configuration. One API call. No ML expertise. No search engineer.

```python
result = db.optimize_search(eval_pairs, budget_hours=8)
# Next morning: search is tuned for your domain
```

This was originally designed as an internal tool for benchmarking. This RFC promotes it to a first-class user-facing feature.

---

## Why This Is Only Possible With Branching

Every other approach to search optimization requires copying your entire dataset for each experiment:

| System | How to tune search | Cost per experiment |
|--------|-------------------|-------------------|
| Elasticsearch | Change config, reindex, evaluate | O(n) reindex time + full data copy |
| Pinecone | Not tunable | N/A |
| Weaviate | Rebuild index with new parameters | O(n) rebuild |
| Solr | Manual config, restart, evaluate | O(n) reload |
| **Strata** | Fork branch, apply recipe, evaluate | **O(1) fork** |

At O(1) per experiment, Strata can run 7,000 experiments overnight. At O(n) per experiment, competitors can run maybe 10 in the same time. That's a 700x advantage in search optimization throughput.

---

## User API

### Python SDK

```python
# Step 1: Define evaluation pairs
# These are examples of "good" search results for given queries.
# 50-100 pairs is enough. More is better but not required.
eval_pairs = [
    # (query, list of expected result keys)
    ("security incidents last week", ["doc:incident-42", "doc:incident-43"]),
    ("Alice's access history", ["event:login-alice-0312", "kv:alice:permissions"]),
    ("network topology changes", ["graph:node:router-7", "json:config:network-v3"]),
    ("failed deployments in staging", ["event:deploy-fail-0308", "kv:deploy:staging:latest"]),
    # ... 50-100 pairs
]

# Step 2: Optimize (one call)
result = db.optimize_search(
    eval_pairs,
    metric="ndcg@10",          # optimization target
    budget_hours=8,             # wall-clock time limit
    max_experiments=2000,       # experiment count limit
    search_space="full",        # what to optimize (see below)
)

# Step 3: Review results
print(result.baseline_score)       # 0.42 NDCG@10
print(result.optimized_score)      # 0.71 NDCG@10
print(result.improvement)          # +69%
print(result.experiments_run)      # 1,847
print(result.best_recipe)          # the winning SearchRecipe
print(result.top_10_recipes)       # runner-ups for comparison
print(result.convergence_round)    # stopped improving at round 12

# The winning recipe is automatically saved to strata.toml
# and becomes the default for db.search()

# Step 4: Verify (optional)
# Run the winning recipe against a held-out test set
test_score = db.evaluate_search(test_pairs, recipe="optimized")
```

### CLI

```bash
# Provide eval pairs as JSONL file
strata search optimize \
  --eval-set eval_pairs.jsonl \
  --metric ndcg@10 \
  --budget-hours 8 \
  --max-experiments 2000

# Check optimization status (while running)
strata search optimize --status

# Review results
strata search optimize --results

# Evaluate a recipe against eval pairs
strata search evaluate --eval-set test_pairs.jsonl --recipe optimized

# Reset to default recipe
strata search optimize --reset
```

### Eval Set Format (JSONL)

```jsonl
{"query": "security incidents last week", "expected": ["doc:incident-42", "doc:incident-43"]}
{"query": "Alice's access history", "expected": ["event:login-alice-0312", "kv:alice:permissions"]}
{"query": "network topology changes", "expected": ["graph:node:router-7", "json:config:network-v3"]}
```

---

## What Gets Optimized

### Search Space Levels

```python
db.optimize_search(eval_pairs, search_space="parameters")  # fastest
db.optimize_search(eval_pairs, search_space="recipes")      # medium
db.optimize_search(eval_pairs, search_space="full")         # most thorough (default)
```

**Level 1: Parameters** (~100 experiments)
Optimize parameters within the current recipe structure:

| Parameter | Search Range | Default |
|-----------|-------------|---------|
| BM25 k1 | 0.5 — 2.0 | 0.9 |
| BM25 b | 0.1 — 1.0 | 0.4 |
| BM25 per-field weights | 0.0 — 3.0 per field | 1.0 |
| RRF k | 10 — 200 | 60 |
| Rerank top_n | 5 — 50 | 20 |
| Rerank blend weight | 0.0 — 1.0 | 0.5 |
| Recency half_life | 1h — 720h | 168h |
| Expansion strategy | lex / semantic / hyde / full | full |
| Expansion count | 1 — 5 | 3 |
| Vector k multiplier | 1x — 10x | 3x |

**Level 2: Recipes** (~500 experiments)
Optimize pipeline structure — which stages, what order:

- Include/exclude expansion stage
- Include/exclude graph traversal stage
- Include/exclude reranking stage
- Include/exclude recency boost
- Graph strategy: BFS vs PageRank vs PersonalizedPageRank
- Fusion method: RRF vs linear combination
- Stage ordering variations

**Level 3: Full** (~2000 experiments)
Both parameters and structure, with cross-interactions:

- Does HyDE expansion help more with graph-boosted results or without?
- Is reranking more valuable after RRF or after graph boost?
- What recency decay curve works best with this expansion strategy?
- These interactions can only be discovered by testing combinations.

---

## Optimization Algorithm

### Tournament Selection with Elitism

```
OPTIMIZE(eval_pairs, budget):

1. BASELINE
   - Evaluate default recipe against eval_pairs
   - Record baseline score
   - Initialize population with default + random mutations

2. GENERATION LOOP (until budget exhausted or converged)
   For each generation:

   a. GENERATE candidates
      - AI planner reads previous results
      - Generates N candidate configurations
      - Mix of: random mutations, crossovers of top performers,
        targeted parameter sweeps around promising regions

   b. EVALUATE in parallel
      - Fork N branches (O(1) each)
      - Apply candidate recipe to each branch
      - Run eval_pairs against each branch
      - Compute metric (NDCG@10, MAP@10, etc.)
      - Record results

   c. SELECT
      - Rank by metric
      - Keep top 20% (elitism)
      - These seed the next generation

   d. CONVERGE check
      - If best score hasn't improved in 3 generations → stop
      - If improvement < 0.1% → stop

3. VALIDATE
   - Run best recipe against full eval set (not fast subset)
   - Compare against baseline
   - If improvement > threshold → promote

4. APPLY
   - Write winning recipe to strata.toml
   - Set as default_recipe
   - Record optimization metadata
   - Clean up experiment branches
```

### Fast Evaluation Subset

For Level 3 (full) optimization, evaluating 2,000 recipes × 100 queries × full search is expensive. The optimizer uses a fast subset:

```
Fast subset selection:
1. Pick 3 corpora: smallest eval pairs, medium, most diverse query types
2. Typical eval time: ~5-10 seconds per recipe
3. At 2,000 recipes: ~3-6 hours (fits in overnight budget)
4. Final validation: full eval set on the winning recipe (~1 minute)
```

### Correlation Validation

Before trusting the fast subset, the optimizer validates that fast subset scores correlate with full eval set scores (Spearman ρ > 0.85). If correlation is weak, it falls back to larger subsets.

---

## Configuration Output

The optimization result is written to `strata.toml` in the database directory:

```toml
# strata.toml
# Everything in this file is the database's configuration.
# AutoResearch writes the [search] section. Users can hand-edit any value.

durability = "standard"
auto_embed = true
embed_batch_size = 512
provider = "local"

# ─── Search Configuration ───
# Hand-tuned or auto-generated by db.optimize_search()

[search]
default_recipe = "optimized"

[search.bm25]
k1 = 0.82
b = 0.35

[search.fusion]
method = "rrf"
k = 55

[search.rerank]
enabled = true
model = "cross-encoder"
top_n = 15
blend = "position_aware"

[search.boost]
recency_enabled = true
recency_half_life_hours = 72.0
graph_proximity_enabled = true
graph_proximity_max_hops = 2

[search.expand]
enabled = true
strategy = "hyde"
n = 2

# ─── Recipes ───
# Named recipes. The default "optimized" recipe is auto-generated.
# Users can add their own recipes here.

[[search.recipes]]
name = "optimized"
description = "AutoResearch-generated, 2026-03-20, NDCG@10: 0.71 (baseline: 0.42)"
stages = [
    { type = "expand", strategy = "hyde", n = 2 },
    { type = "bm25", k = 45, k1 = 0.82, b = 0.35 },
    { type = "vector", k = 50 },
    { type = "fuse", method = "rrf", k = 55 },
    { type = "boost", signal = "recency", half_life_hours = 72.0 },
    { type = "rerank", top_n = 15, blend = "position_aware" },
]
limit = 10
budget_ms = 2000

[[search.recipes]]
name = "fast"
description = "Low-latency keyword search"
stages = [
    { type = "bm25", k = 20 },
]
limit = 10
budget_ms = 50

# ─── Optimization Metadata ───
# Written by db.optimize_search(). Tracks what was optimized and when.

[search.optimization]
last_run = "2026-03-20T04:30:00Z"
experiments_run = 1847
baseline_ndcg10 = 0.42
optimized_ndcg10 = 0.71
improvement_pct = 69.0
search_space = "full"
convergence_round = 12
total_rounds = 15
eval_set_hash = "sha256:a1b2c3d4e5..."
eval_set_size = 87
budget_hours = 8.0
wall_clock_hours = 6.2
```

### Properties of strata.toml

- **Single file.** One config for everything — durability, embedding, search, recipes, optimization history.
- **Human-readable and editable.** TOML is simple. A user can hand-tune any parameter AutoResearch generated.
- **Inside `.strata`.** Lives in the database directory. When you `strata clone` from StrataHub, the tuned config comes with the data. Search is pre-optimized.
- **Eval set hash.** Detects data drift. If the hash changes significantly, Strata can suggest re-optimization.
- **Recipes are inline.** Named recipes live in the config, not in a separate store. Visible, editable, version-controlled with the database.
- **Optimization metadata.** Full provenance: when it ran, how many experiments, what improved, how long it took. Auditable.

---

## StrataHub Integration

This is where AutoResearch becomes a network effect.

When a user optimizes search on their dataset and pushes to StrataHub, the `strata.toml` goes with it. Anyone who clones that database gets pre-optimized search for free.

```bash
# Creator optimizes
db.optimize_search(eval_pairs, budget_hours=8)
strata push stratadb.ai/climate/noaa-ocean-temps

# Consumer gets optimized search instantly
strata clone stratadb.ai/climate/noaa-ocean-temps
db.search("sea surface temperature anomalies 2025")  # uses creator's optimized recipe
```

Over time, StrataHub databases become *curated knowledge systems* — not just data, but data with optimized retrieval. The more people optimize and share, the more valuable the network becomes.

---

## Drift Detection and Re-Optimization

Data changes over time. An optimization run on January's data may not be optimal for March's data.

```python
# Check if optimization is stale
status = db.search_optimization_status()
print(status.is_stale)              # True if data changed significantly
print(status.data_change_pct)       # 34% of keys changed since last optimization
print(status.eval_set_coverage)     # 72% of eval queries still have expected results
print(status.estimated_score_now)   # projected current NDCG based on sampling

# Re-optimize if stale
if status.is_stale:
    db.optimize_search(eval_pairs, budget_hours=4)  # faster — warm start from previous best
```

### Warm Start

Re-optimization doesn't start from scratch. It uses the current best recipe as the baseline and explores mutations around it. This typically converges 3-5x faster than cold start.

---

## Competitive Analysis

| Capability | Strata | Elasticsearch | Pinecone | Weaviate |
|-----------|--------|---------------|----------|----------|
| Self-optimizing search | One API call | Manual, requires search engineer | Not tunable | Limited auto-tuning |
| Experiments per night | 7,000 | ~10 (manual) | 0 | ~10 |
| Pipeline structure search | Yes (recipe-level) | No | No | No |
| Optimization cost | O(1) per experiment (branching) | O(n) per experiment (reindex) | N/A | O(n) |
| Config portability | strata.toml travels with data | Cluster config, separate from data | N/A | Separate config |
| Eval set driven | Yes (user provides pairs) | Manual relevance judgments | N/A | No |

---

## Implementation Phases

### Phase 1: Evaluation Framework (~1 week)

- Eval set data model: `Vec<(String, Vec<String>)>` (query, expected keys)
- Eval set persistence: JSONL file + hash computation
- Metric computation: NDCG@10, MAP@10, Recall@100, MRR
- `db.evaluate_search(eval_pairs, recipe)` — evaluate a single recipe
- Baseline measurement against default recipe

### Phase 2: Experiment Runner (~2 weeks)

- Branch-parallel experiment execution
- Candidate generation: random mutations, parameter sweeps
- Tournament selection with elitism
- Convergence detection
- Budget management (time-based and experiment-count-based)
- Experiment cleanup (delete experiment branches after completion)

### Phase 3: strata.toml Integration (~1 week)

- Write winning recipe to `strata.toml` `[search]` section
- Write optimization metadata to `[search.optimization]`
- Read recipes from `strata.toml` on database open
- `default_recipe` selection from config
- Hand-editable: user changes to `strata.toml` override AutoResearch

### Phase 4: User API (~1 week)

- `db.optimize_search()` Python/Node API
- `strata search optimize` CLI
- `strata search evaluate` CLI
- `strata search optimize --status` for in-progress monitoring
- MCP tool: `strata_optimize_search`

### Phase 5: Intelligence (~2 weeks)

- AI planner for candidate generation (reads previous results, proposes targeted experiments)
- Recipe structure mutations (add/remove/reorder stages)
- Cross-interaction discovery (parameter × structure interactions)
- Warm start for re-optimization
- Drift detection and staleness scoring

### Phase 6: StrataHub (~1 week)

- `strata.toml` included in `strata push` / `strata clone`
- StrataHub displays optimization metadata (score, experiment count)
- "Pre-optimized" badge on optimized databases

Total: ~8 weeks

---

## Open Questions

1. **Eval set quality**: Garbage in, garbage out. If the user provides bad eval pairs, the optimizer will overfit to them. Should we validate the eval set? (e.g., check that expected results actually exist, warn if queries are too similar). Recommendation: yes, basic validation + warnings.

2. **Overfitting**: With 100 eval pairs and 2,000 experiments, could the optimizer overfit to the eval set? Mitigation: held-out validation split (80/20), report both train and validation scores, warn if gap > 10%.

3. **Cost of expansion/reranking experiments**: Recipes with LLM expansion or reranking are expensive to evaluate. Should the optimizer have a cost model? Recommendation: yes. Each stage has an estimated cost, and the optimizer balances quality vs latency in its search.

4. **Multi-objective optimization**: Users may want to optimize for both quality (NDCG) and latency (p99). Pareto-front optimization instead of single-metric? Recommendation: single metric for v1 with a latency constraint (budget_ms). Multi-objective in v2.

5. **Continuous optimization**: Instead of one-shot optimization, run AutoResearch continuously in the background, adapting as data changes. Recommendation: defer. One-shot with drift detection is simpler and sufficient for v1.

---

## Success Criteria

1. User with 50 eval pairs and 8 hours of budget sees measurable improvement (>10% NDCG@10) on a real dataset
2. Optimization is fully automated — no ML expertise required
3. Optimized config persists in `strata.toml` and survives restarts
4. `strata clone` from StrataHub gets pre-optimized search
5. Hand-edits to `strata.toml` are respected (AutoResearch doesn't overwrite manual tuning unless explicitly re-run)
