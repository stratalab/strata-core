# Strata Search Benchmark Plan

**Goal:** Establish credible, reproducible search quality numbers for every level of the Strata retrieval pipeline, with honest reporting of quality, latency, and cost tradeoffs.

**Audiences:** Academic reviewers (paper), practitioners (technical evaluation), investors (technical depth).

**Principle:** Report everything — wins, losses, and costs. Show where each capability helps and where it hurts. Transparent discussion of limitations increases trust.

---

## 1. The Ablation Ladder

Each level adds exactly one capability. We measure the marginal contribution of every component in isolation.

| Level | Recipe | What it tests | Models needed |
|---|---|---|---|
| L1 | `keyword` | BM25 baseline — is our inverted index competitive? | None |
| L2 | `semantic` | Vector-only — are our embeddings + HNSW competitive? | Embed model |
| L3 | `hybrid` | BM25 + vector + RRF — does fusion actually help? | Embed model |
| L4 | L3 + expansion (lex only) | Keyword synonym expansion value | Embed + expand model |
| L5 | L3 + expansion (vec only) | Semantic reformulation expansion value | Embed + expand model |
| L6 | L3 + expansion (hyde only) | HyDE expansion value | Embed + expand model |
| L7 | L3 + all expansions | Combined expansion — lex + vec + hyde | Embed + expand model |
| L8 | L3 + reranking | Reranking value isolated (no expansion) | Embed + rerank model |
| L9 | L7 + reranking (`default`) | Full text pipeline — expansion + reranking together | All local models |
| L10 | BM25 + graph (PPR) | Graph retrieval value over keyword-only | None |
| L11 | L3 + graph | Full multi-signal — BM25 + vector + graph + RRF | Embed model |
| L12 | L9 + graph | Everything — full pipeline + graph | All local models |

### What we measure at every level

| Metric | Why |
|---|---|
| **nDCG@10** | Primary quality metric (BEIR standard) |
| **Recall@100** | Upper bound for reranking — did we find the relevant docs? |
| **QPS** | Throughput — queries per second |
| **p50 / p95 / p99 latency** | Tail latency — worst case user experience |
| **Per-query nDCG@10 distribution** | Which queries improved, degraded, or stayed the same |

### Per-query analysis (critical)

Averages hide everything. For each step up the ladder, we report:
- % of queries that **improved** (delta > +0.01)
- % of queries that **degraded** (delta < -0.01)
- % of queries **unchanged**
- Distribution plot (histogram of per-query nDCG deltas)

This is how we honestly show: "expansion helps 60% of queries but hurts 15%, net effect is +X."

---

## 2. Datasets

### Full BEIR suite (13 public datasets)

All levels L1-L3 (keyword, semantic, hybrid) run on the full suite. This establishes BM25 parity with Pyserini.

| Dataset | Domain | Corpus size | Queries | Notes |
|---|---|---|---|---|
| NFCorpus | Medical/nutrition | 3.6K | 323 | Tiny, volatile — dense graded judgments |
| SciFact | Scientific claims | 5.2K | 300 | Small, well-curated |
| ArguAna | Arguments | 8.7K | 1,406 | BM25 is essentially unbeatable |
| TREC-COVID | COVID research | 172K | 50 | Pooling bias — novel docs penalized |
| FiQA | Finance QA | 57K | 648 | Domain-specific |
| Quora | Duplicate questions | 523K | 10K | Too easy (BM25 = 0.789) |
| DBPedia | Entity descriptions | 4.6M | 400 | Large corpus |
| NQ (Natural Questions) | Wikipedia QA | 2.7M | 3,452 | Google-sourced, factoid |
| HotpotQA | Multi-hop QA | 5.2M | 7,405 | 2-hop reasoning |
| FEVER | Fact verification | 5.4M | 6,666 | Binary relevance |
| Climate-FEVER | Climate claims | 5.4M | 1,535 | Noisy labels |
| ScIDocs | Scientific docs | 25K | 1,000 | Citation-based relevance |
| Touche-2020 | Arguments | 382K | 49 | Problematic — BM25 unbeatable |

**Target:** BM25 macro-average >= 0.429 (Pyserini multifield baseline).

### Representative subset (8 datasets) for expensive ablations

L4-L12 (expansion, reranking, graph) run on this subset to keep costs reasonable:

**NFCorpus, SciFact, FiQA, TREC-COVID, NQ, HotpotQA, ArguAna, DBPedia**

Chosen to span: small/large corpora, easy/hard queries, medical/finance/general domains, single-hop/multi-hop.

### Graph-specific datasets

For L10-L12, we need text + knowledge graph:

| Dataset | Graph | Text | Task |
|---|---|---|---|
| HotpotQA | Wikipedia hyperlinks | Wikipedia passages | 2-hop QA |
| 2WikiMultiHopQA | Wikidata relations | Wikipedia passages | 2-4 hop QA |
| MuSiQue | Composed single-hop | Wikipedia passages | 2-4 hop QA |
| PrimeKG | 129K nodes, 4M edges | Biomedical literature | Medical QA |

Primary graph metric: **Complementary retrieval** — results found by graph but NOT by BM25 + vector. Presented as Venn diagram per dataset.

### RAG datasets (future, after retrieval is solid)

| Dataset | Task | Metrics |
|---|---|---|
| NQ | Short-form QA | EM, F1, context recall |
| ASQA | Ambiguous QA with citations | Citation recall/precision (ALCE) |
| HotpotQA | Multi-hop QA | EM, F1, supporting facts F1 |
| ELI5 | Long-form QA | Faithfulness (RAGAS), citation accuracy |

---

## 3. Baselines

### BM25 baseline (authoritative)

**Pyserini BM25 Multifield** (k1=0.9, b=0.4). This is the number every reviewer knows. We use the exact same parameters.

Per-dataset reference numbers from Kamalloo et al. (SIGIR 2024):

| Dataset | Pyserini BM25 |
|---|---|
| NFCorpus | 0.3218 |
| SciFact | 0.6647 |
| ArguAna | 0.3970 |
| TREC-COVID | 0.5947 |
| FiQA | 0.2361 |
| Quora | 0.7886 |
| DBPedia | 0.3180 |
| NQ | 0.3055 |
| HotpotQA | 0.6027 |
| FEVER | 0.6513 |
| Climate-FEVER | 0.1651 |
| ScIDocs | 0.1490 |
| Touche-2020 | 0.4422 |
| **Macro avg** | **0.429** |

### Embedding baselines

| Model | BEIR Retrieval avg | Notes |
|---|---|---|
| all-MiniLM-L6-v2 (our default) | ~0.41-0.43 | Fastest, 256-token limit |
| BGE-M3 | ~0.50-0.53 | Best local option |
| OpenAI text-embedding-3-small | ~0.49-0.51 | Cloud baseline |

### Reranking baselines

| Approach | Expected BEIR avg | What it tests |
|---|---|---|
| No reranking | (hybrid baseline) | Control |
| Naive replacement | (measure) | Cross-encoder scores replace fusion scores entirely |
| Linear interpolation | (sweep alpha) | alpha * fusion + (1-alpha) * reranker |
| Position-aware blending (ours) | (measure) | Tiered weights: 0.75 / 0.60 / 0.40 |

The comparison between naive, linear, and position-aware is the key claim. If position-aware doesn't beat naive, the feature doesn't justify its complexity.

---

## 4. Evaluation Tooling

### Metrics computation

Use **`ir_measures`** or **`pytrec_eval`** for all quality metrics. Not our custom nDCG implementation — reviewers won't trust it.

For statistical significance: **paired t-test with Bonferroni correction** across datasets. Report p-values. Delta is only meaningful if p < 0.05.

### Latency measurement

Use **`criterion`** (Rust) for per-query latency histograms. Report p50/p95/p99 from the distribution. QPS = total queries / total wall time (not mean of per-query rates).

### RAG evaluation (future)

**RAGAS** for faithfulness + context recall. **ALCE** for citation recall/precision. Report retrieval and generation metrics separately — never conflate them.

---

## 5. Reporting

### Per-audience output

**Paper:** Full table (datasets in rows, levels in columns), per-dataset heatmap (green = improvement over previous level, red = degradation), statistical significance markers. Radar chart normalized to BM25 baseline.

**Practitioner docs:** Quality-latency-cost table per recipe. "If you care about speed, use `keyword`. If you care about quality, use `default`. Here's exactly what you gain and what it costs."

**Investor deck:** Macro-average nDCG@10 per level, competitive feature matrix, Pareto curve (quality vs latency).

### What we always report

1. **All 13 datasets** for BM25 — including ones where we underperform
2. **Per-query distributions** — not just averages
3. **Latency at every level** — quality without speed context is meaningless for embedded
4. **Where each capability hurts** — expansion degradation %, reranking rank inversions, graph noise on keyword-precise queries
5. **Model versions and parameters** — exact GGUF quantization, exact k1/b, exact RRF k

### What we never do

- Cherry-pick datasets (Roque et al. proved 46% of methods can look best by picking 4 datasets)
- Report only the best run (always mean +/- std over 3+ runs)
- Compare embedded latency against server systems without acknowledging the advantage
- Tune parameters on the test set (BEIR is zero-shot — tune on MS MARCO if needed)
- Conflate retrieval quality with RAG answer quality

---

## 6. Execution Phases

### Phase 1: BM25 parity (this week)

1. Build the BEIR harness — all 13 public datasets, automated download, indexing, evaluation
2. Run L1 (`keyword`) on all 13 datasets
3. Compare against Pyserini baselines per-dataset
4. Fix any tokenizer/stemmer issues that cause > 0.005 delta
5. Target: macro-average within 0.01 of Pyserini's 0.429

**Deliverable:** Table with 13 rows, Strata vs Pyserini columns, delta column.

### Phase 2: Embedding + hybrid (next)

1. Run L2 (`semantic`) with MiniLM on the 8-dataset subset
2. Run L3 (`hybrid`) — BM25 + MiniLM + RRF
3. Measure fusion lift: is hybrid > max(keyword, semantic)?
4. Sweep RRF k in {20, 40, 60, 80, 100} on 2-3 datasets to validate k=60 default

**Deliverable:** Table with L1/L2/L3 rows, 8 dataset columns, QPS column.

### Phase 3: Intelligence layer ablation

1. Run L4-L9 on 8-dataset subset
2. Per-query analysis: which queries benefit from expansion, which from reranking
3. Strong signal detection validation: partition queries by BM25 confidence, verify skip-expansion is correct
4. Reranking comparison: naive vs linear vs position-aware
5. Report expansion model load time and per-query overhead

**Deliverable:** Full ablation table L1-L9, per-query delta distributions, reranking method comparison.

### Phase 4: Graph (after PPR is wired into substrate)

1. Build knowledge graphs from HotpotQA/2WikiMultiHopQA hyperlinks
2. Run L10-L12
3. Complementary retrieval analysis: Venn diagram of what graph finds that text doesn't
4. Stratify by hop count — graph value should increase with reasoning depth

**Deliverable:** Graph ablation table, complementary retrieval Venn diagrams, hop-count analysis.

### Phase 5: RAG (after RAG pipeline is built)

1. Run text-only RAG vs GraphRAG on HotpotQA, NQ, ASQA
2. Evaluate with RAGAS (faithfulness, context recall) + ALCE (citations)
3. Gold-context experiment: give the generator perfect retrieval to isolate generation quality
4. Report retrieval metrics and generation metrics separately

### Phase 6: Systems comparison

1. Fork search-benchmark-game, add Strata alongside Tantivy
2. BM25 latency comparison on English Wikipedia
3. Run on identical hardware with documented specs
4. Report per-query-type latency (single term, phrase, AND, OR, fuzzy)

---

## 7. Novel Contributions

Things nobody else has published that we can claim:

1. **GGUF quantization impact on retrieval quality.** Run Q4_K_M, Q5_K_M, Q8_0, F16 on BEIR retrieval tasks. Report nDCG@10 delta per quantization level. Expected: first published MTEB-style evaluation of quantized embeddings.

2. **Position-aware reranking blending.** If our tiered blending (0.75/0.60/0.40) beats naive replacement and linear interpolation on BEIR, that's a publishable technique. The "Lost in the Middle" problem provides theoretical grounding.

3. **Grammar-constrained query expansion.** GBNF-constrained generation producing typed variants (lex/vec/hyde) with hallucination guard. If this matches or beats unconstrained expansion at lower cost, that's novel.

4. **Embedded search quality parity.** If an in-process embedded library matches Pyserini BM25 and competitive hybrid search on BEIR, that's the first demonstration that embedded != quality compromise.

5. **Temporal search evaluation.** First benchmark of point-in-time retrieval and temporal diff using TemporalWiki + TimeQA. Novel evaluation methodology.
