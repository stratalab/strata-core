# Self-Optimizing Search: Autonomous Research via Branch-Parallel Experimentation

## Abstract

We introduce a research methodology for search pipeline optimization that combines three capabilities unique to Strata: (1) copy-on-write branch isolation for zero-cost parallel experimentation, (2) a modular search pipeline where every stage is independently toggleable, and (3) an autonomous AI agent that runs hundreds of experiments per night using evolutionary tournament selection over search configurations.

The result is a **self-optimizing database** — the first search system that uses its own branching primitive to autonomously discover better retrieval strategies, validated against the industry-standard BEIR benchmark.

No other system can do this. Elasticsearch cannot branch its index. Pinecone cannot fork a collection. Every competing approach requires N copies of the data at N times the storage cost. Strata's copy-on-write architecture makes branching nearly free, enabling a class of autonomous optimization that is architecturally impossible elsewhere.

---

## Motivation

### The Problem with Manual Search Tuning

Search pipeline optimization is traditionally a manual, expert-driven process:

1. An engineer hypothesizes that changing BM25 k1 from 0.9 to 1.2 might help
2. They modify the code, rebuild, re-index, and run an evaluation suite
3. They inspect the results, decide whether to keep the change
4. They repeat, one experiment at a time

This process is slow (~4-6 experiments per day), biased toward the engineer's intuition, and explores a tiny fraction of the configuration space. A search pipeline with 15 tunable parameters has a combinatorial explosion of possible configurations. Manual exploration covers maybe 0.01% of the space.

### The Autoresearch Paradigm

Karpathy's [autoresearch](https://github.com/karpathy/autoresearch) demonstrated that an AI agent, given a constrained modification surface and a clear objective metric, can run ~100 experiments overnight — 10-20x more than a human researcher. The agent discovers non-obvious improvements because it explores without human bias.

The key ingredients:
- **Fixed time budget** per experiment (comparable results across configurations)
- **Single objective metric** (no ambiguity about what "better" means)
- **Constrained modification surface** (agent can't break the system)
- **Autonomous loop** (modify → evaluate → keep/discard → repeat)
- **Human steering** via a `program.md` document (the "research org code")

### Strata's Unique Advantage: Branch-Parallel Experimentation

Standard autoresearch is **sequential** — one experiment at a time. Strata's copy-on-write branching makes it **parallel**.

Instead of:
```
experiment 1 → evaluate → experiment 2 → evaluate → ... (sequential, ~100/night)
```

Strata enables:
```
fork N branches → N experiments in parallel → evaluate all → merge winner (parallel, ~800/night)
```

Branching cost is near-zero because Strata's storage layer shares data across branches via copy-on-write. Forking a 5-million-document database creates no data copies — only the modified index pages are written. This means the parallelism scales with available CPU cores, not with storage capacity.

---

## Architecture

### System Overview

```
                         ┌──────────────────────┐
                         │   program.md          │
                         │   (human steers)      │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │   Experiment Planner  │
                         │   (AI agent)          │
                         │   Reads program.md    │
                         │   Generates N configs │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
              ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
              │  Branch A  │  │  Branch B  │  │  Branch C  │  ... Branch N
              │  Config A  │  │  Config B  │  │  Config C  │
              │            │  │            │  │            │
              │  Index     │  │  Index     │  │  Index     │
              │  Search    │  │  Search    │  │  Search    │
              │  Evaluate  │  │  Evaluate  │  │  Evaluate  │
              └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
                    │               │               │
                    └───────┬───────┴───────┬───────┘
                            │               │
                     ┌──────▼──────┐  ┌─────▼──────┐
                     │  Selection  │  │  Discard    │
                     │  (best      │  │  losers     │
                     │   nDCG@10)  │  │             │
                     └──────┬──────┘  └────────────┘
                            │
                     ┌──────▼──────┐
                     │  Merge to   │
                     │  main       │
                     └──────┬──────┘
                            │
                            ▼
                      Next round
```

### Components

**1. Program Document (`program.md`)**

Human-authored guidance that steers the agent's exploration. Updated between rounds or phases. Examples:

```markdown
# Round 1: BM25 Parameter Exploration
Explore BM25 scoring parameters. The current baseline uses k1=0.9, b=0.4
(Anserini defaults). Try variations across both parameters. Also experiment
with the title boost factor (currently 1.2x).

Constraints:
- Only modify scorer parameters, not tokenizer or index structure
- k1 range: [0.5, 2.5]
- b range: [0.1, 0.9]
- title_boost range: [1.0, 5.0]
```

```markdown
# Round 4: Tokenizer Variations
The current tokenizer uses Porter stemming with 33 Lucene stopwords.
Explore alternatives:
- Snowball stemmer (more aggressive)
- Expanded stopword lists (SMART 571-word)
- Compound word preservation (hyphenated terms)
- Combinations of the above

Constraint: Do not change scorer parameters (locked from Round 2 winner).
```

```markdown
# Round 8: Full Combinatorial
Previous rounds found optimal scorer params, tokenizer config, and
fusion weights independently. Now explore combinations — some parameters
may interact. Try the top-3 scorers × top-3 tokenizers × top-3 fusion
configs = 27 combinations.
```

**2. Experiment Planner (AI Agent)**

Reads `program.md`, the current search implementation, and previous experiment results. Generates N candidate configurations for the current round. The planner:

- Understands the search pipeline architecture
- Knows which parameters are locked vs explorable
- Reviews previous round results to guide exploration (no repeat experiments)
- Balances exploration (novel configs) with exploitation (variations on winners)
- Outputs structured experiment configs (JSON)

**3. Branch-Parallel Executor**

For each candidate configuration:
1. Fork a Strata branch from `main` (near-instant, CoW)
2. Apply the configuration (modify index settings, scorer params, tokenizer, etc.)
3. Re-index if needed (only for structural changes like tokenizer/stemmer)
4. Run the BEIR fast evaluation subset
5. Record results (nDCG@10 per dataset + timing)
6. Report back to the selector

All N branches run in parallel, bounded by available CPU cores.

**4. Tournament Selector**

After all branches complete:
1. Rank by average nDCG@10 across the evaluation datasets
2. If the winner improves over the current `main` baseline: merge
3. If no improvement: agent reviews results, adjusts strategy, tries again
4. Log all results (winners and losers) to the experiment history

**5. Validation Gate**

After convergence (no improvement for K rounds), run the full BEIR 15-dataset suite on the final configuration. This is the publication-quality result that goes into the ablation table.

---

## The Evolutionary Loop

### Round Structure

Each round follows tournament selection:

```
Round R:
  1. Agent reads program.md + history of rounds 1..R-1
  2. Agent generates N candidate configs (mutations from current best)
  3. Fork N branches from main
  4. Run BEIR fast subset on all N branches (parallel)
  5. Rank by nDCG@10
  6. If best > main baseline:
       merge winner to main
       log: "Round R: +0.0XX nDCG@10 from [description of change]"
  7. If best <= main baseline:
       log: "Round R: no improvement (best tried: [descriptions])"
       agent adjusts strategy for Round R+1
  8. Discard all non-winning branches
  9. Repeat
```

### Mutation Strategies

The agent applies different mutation strategies depending on the phase:

**Point mutations**: Change a single parameter (k1, b, stopword list, stemmer choice).

**Crossover**: Combine winning elements from previous rounds (e.g., Round 2's scorer + Round 4's tokenizer).

**Structural mutations**: Enable/disable pipeline stages (add sparse expansion, toggle proximity scoring, enable BM25F).

**Ablation mutations**: Remove one component from the current best to measure its contribution.

**Random exploration**: Occasionally try configurations far from the current optimum to escape local minima.

### Convergence

A phase converges when:
- No improvement for 5 consecutive rounds, OR
- The improvement in the last 10 rounds is < 0.001 nDCG@10 cumulative, OR
- The time budget for the phase is exhausted

After convergence, the agent:
1. Runs the full BEIR validation suite
2. Records the result in the ablation table
3. Advances to the next phase (per `program.md`)

---

## Fast Evaluation Subset

The inner loop metric must be fast enough for rapid iteration (~2-3 minutes per experiment). We use a curated subset of BEIR datasets that correlates with full-suite performance:

| Dataset | Docs | Queries | Index Time | Search Time | Total |
|---------|------|---------|------------|-------------|-------|
| NFCorpus | 3,633 | 323 | ~0.5s | ~0.2s | ~0.7s |
| SciFact | 5,183 | 300 | ~0.7s | ~0.2s | ~0.9s |
| FiQA | 57,638 | 648 | ~5s | ~0.5s | ~5.5s |

**Total fast eval: ~7 seconds** (+ overhead for branch creation, configuration, teardown).

With 8-way parallelism and ~30s per round (including overhead), this yields:
- ~120 rounds per hour
- ~960 experiments per hour (8 branches per round)
- **~7,000 experiments overnight (8 hours)**

This is **70x** the throughput of sequential autoresearch and **1,000x** the throughput of manual experimentation.

### Correlation Validation

Before relying on the fast subset, we validate that it correlates with full BEIR performance:
1. Run 20 diverse configurations on both the fast subset and full 15-dataset suite
2. Compute Spearman rank correlation between fast-subset nDCG@10 and full-suite nDCG@10
3. Require correlation > 0.85 to trust the fast subset as a proxy

If correlation is insufficient, add datasets to the fast subset (ArguAna 8.7K, SciDocs 25K) until it reaches the threshold.

---

## Research Phases

The autoresearch loop operates within each phase of the search pipeline implementation plan (#1484). The phases provide the architectural scaffolding; the autonomous loop optimizes within each phase.

### Phase A: BM25 Optimization

**program.md focus**: Scorer parameters, tokenizer configuration, stopword lists, stemmer choice.

**Modification surface**: `BM25LiteScorer` parameters, `tokenizer.rs` configuration, stopword list selection.

**Locked**: Index structure, fusion parameters, expansion/reranking.

**Expected exploration**:
- k1 × b grid search (25 combinations)
- Stemmer variants (Porter, Snowball, none)
- Stopword lists (Lucene 33, SMART 571, custom)
- Title boost factor sweep
- Compound word handling (on/off)
- Combinations of above (~200+ experiments)

**Success criterion**: nDCG@10 on fast subset exceeds Pyserini BM25-multifield.

### Phase B: Hybrid Fusion Optimization

**program.md focus**: RRF parameters, weight ratios, embedding model selection.

**Modification surface**: `RRFFuser` k_rrf, original query weight, top-rank bonuses, embedding model choice.

**Locked**: BM25 parameters (locked from Phase A winner), expansion/reranking.

**Expected exploration**:
- k_rrf sweep × weight ratio sweep (25 combinations)
- Top-rank bonus calibration
- Embedding model comparison (MiniLM-L6, MiniLM-L12, nomic-embed, bge-small)
- Combinations (~150+ experiments)

### Phase C: Write-Time Sparse Expansion

**program.md focus**: Sparse expansion parameters — vocabulary size, expansion weight, top-K terms.

**Modification surface**: `sparse_expansion.rs` parameters, expansion weight in `PostingEntry`.

**Locked**: BM25 + fusion parameters from Phases A-B.

**Expected exploration**:
- Vocabulary size (10K, 25K, 50K, 100K)
- Expansion weight (0.1, 0.2, 0.3, 0.5)
- Top-K expanded terms per document (5, 10, 20, 50)
- Combinations (~100+ experiments)

### Phase D: LLM-Enhanced Pipeline

**program.md focus**: Expansion prompts, reranking strategy, blending weights.

**Modification surface**: Expansion prompt templates, drift guard thresholds, blending weight matrix, rerank candidate count.

**Locked**: BM25 + fusion + sparse expansion from Phases A-C.

**Expected exploration**:
- Expansion prompt variations
- Drift guard strictness (1 shared term, 2 shared stems, cosine threshold)
- Blending weight matrix (position-dependent, 3x3 grid per position band)
- Cross-encoder vs LLM-as-judge comparison
- Strong signal threshold calibration
- Combinations (~300+ experiments)

### Phase E: Graph-Augmented Search

**program.md focus**: Graph boost weight, max hops, three-signal blending weights.

**Modification surface**: Graph proximity calculation, boost weight, blending formula.

**Locked**: All prior optimizations.

**Note**: Requires a graph-structured benchmark (not BEIR). Uses a custom evaluation dataset with known entity relationships.

### Phase F: Full Combinatorial

**program.md focus**: Cross-phase interactions. Parameters optimized independently may interact.

**Modification surface**: All parameters unlocked.

**Expected exploration**: Top-3 configs from each prior phase crossed = 3^5 = 243 combinations, plus agent-generated novel combinations.

---

## Why This Methodology is Novel

### 1. The Database Optimizes Itself

The most striking aspect: **Strata uses its own branching primitive to optimize its own search quality.** The optimization tool is the thing being optimized. This is only possible because branching is a first-class database operation, not an external tool.

Other databases would need:
- N separate instances (N x memory, N x disk)
- An external orchestrator to manage copies
- Data loading and indexing for each copy
- Cleanup after experiments

Strata needs:
- One `branch_create` call per experiment (microseconds, near-zero storage)
- Parallel search execution on branches (native, no orchestration)
- One `branch_merge` call for the winner
- Losers garbage-collected automatically

### 2. Structural Advantage Over Sequential Methods

Sequential autoresearch explores a linear path through configuration space. Branch-parallel autoresearch explores a **tree** — each round evaluates N candidates simultaneously, and the search branches at every step.

The analogy is beam search vs greedy search in language models. Greedy search (sequential autoresearch) commits to each step. Beam search (branch-parallel) maintains multiple candidates and selects the best path.

With N=8 branches per round and R rounds:
- Sequential: explores R configurations
- Branch-parallel: explores R × N configurations
- Branch-parallel with history: agent learns from all R × N experiments, not just the winning path

### 3. Reproducibility by Construction

Every experiment is:
- A named Strata branch (immutable snapshot of the configuration)
- A BEIR evaluation result (deterministic metric)
- Linked to a specific git commit of the search code
- Recorded in the experiment log with full configuration

Any result can be reproduced by checking out the git commit and the Strata branch. The branch IS the experiment — it contains the exact index state, scorer parameters, and configuration that produced the result.

### 4. The Agent Learns from Failures

Unlike grid search or random search, the AI agent observes all results (winners AND losers) and uses them to inform future experiments. After 50 rounds, the agent has seen 400 configurations and understands the landscape:

- "High k1 hurts on short-document datasets but helps on long ones"
- "Snowball stemming + SMART stopwords is strictly better than Porter + Lucene"
- "Proximity scoring helps multi-word queries but hurts single-word queries"

This accumulated knowledge guides increasingly targeted exploration — the equivalent of a researcher building intuition, but over hundreds of experiments rather than dozens.

---

## Implementation Requirements

### In strata-core

1. **Per-branch search configuration** — BM25 parameters, tokenizer settings, fusion weights stored as branch-level config (via `CONFIG SET` or `strata.toml` on the branch). Already partially supported.

2. **Per-stage timing in search stats** — `SearchStatsOutput` extended with `stage_latencies_ms` breakdown. Required for latency ablation tables.

3. **Branch-level index rebuild** — `REINDEX` command that rebuilds the inverted index on the current branch with current tokenizer/scorer settings. Needed when tokenizer config changes.

### In strata-eval

4. **Schema v3 result format** — Per-run JSON with full configuration tracking, per-query scores, per-stage latencies. Defined in `docs/beir-ablation-format.md`.

5. **Fast eval mode** — BEIR runner that operates on the NFCorpus + SciFact + FiQA subset with <10s total eval time.

6. **Branch-parallel executor** — Orchestrator that forks N branches, applies configs, runs eval on each, collects results, merges winner. The core autoresearch loop.

7. **Experiment logger** — Append-only log of all experiments (config, result, branch name, round, phase). Feeds the agent's learning.

8. **Convergence detector** — Monitors improvement rate, declares convergence when gains plateau.

### New: autoresearch harness

9. **`program.md` template library** — One template per research phase (A-F), with parameter ranges, constraints, and exploration guidance.

10. **Agent interface** — Structured protocol for the AI agent to propose configurations, receive results, and update strategy. Compatible with Claude Code, MCP, or any agent framework.

11. **Validation gate** — Automated full-BEIR run triggered on convergence, with significance testing against the previous baseline.

---

## Expected Outcomes

### Quantitative

Based on the autoresearch paradigm applied to search:

- **Phase A (BM25)**: +2-5% nDCG@10 over current defaults (parameter optimization alone typically yields this on BEIR)
- **Phase B (Hybrid)**: +1-3% additional from fusion tuning
- **Phase C (Sparse expansion)**: +5-15% additional (ELSER-class improvement)
- **Phase D (LLM-enhanced)**: +3-8% additional from expansion + reranking
- **Phase E (Graph)**: +2-5% on graph-structured benchmarks
- **Phase F (Combinatorial)**: +1-3% from cross-phase interaction discovery

Cumulative estimated improvement: **+15-35% nDCG@10** over the current baseline, discovered autonomously with statistical rigor.

### Qualitative

- A **reproducible experiment log** with thousands of data points mapping the search configuration space
- **Publication-ready ablation tables** with significance tests, generated automatically
- **Optimal configurations per domain** — the agent discovers that medical corpora prefer different parameters than financial corpora
- A **methodology paper** on branch-parallel autoresearch that is independently publishable

---

## Competitive Positioning

This methodology produces a unique claim in the search database market:

> "Strata is the only database that can autonomously optimize its own search quality using branch-parallel experimentation — discovering configurations that outperform hand-tuned systems by running thousands of experiments overnight at near-zero cost."

The key differentiators:

1. **Self-optimization** — The database improves itself (no external tools required)
2. **Zero-cost parallelism** — Copy-on-write branching, not N copies of the data
3. **AI-native methodology** — Designed for agent-driven experimentation, not human point-and-click
4. **Rigorous validation** — BEIR benchmark, significance testing, ablation tables
5. **Reproducible by construction** — Every experiment is a branch snapshot

No competing system — Elasticsearch, Pinecone, Weaviate, Qdrant, Milvus, or any other — can make this claim. They lack the branching primitive that makes it architecturally possible.

---

## References

- [autoresearch](https://github.com/karpathy/autoresearch) — Karpathy, 2025. Autonomous AI research via constrained experimentation
- [BEIR](https://github.com/beir-cellar/beir) — Thakur et al., NeurIPS 2021. Heterogeneous retrieval benchmark
- [Strata Branch Architecture](../architecture/strata-graph.md) — Copy-on-write branching with snapshot isolation
- [Search Pipeline Implementation Plan](search-pipeline-implementation-plan.md) — #1484, phased search improvements
- [Hybrid Search Pipeline](https://github.com/stratalab/strata-core/issues/1269) — #1269, core pipeline design
- [Graph-Augmented Hybrid Search](https://github.com/stratalab/strata-core/issues/1270) — #1270, graph integration RFC
