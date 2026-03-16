# Search Pipeline Implementation Plan

## Current State Assessment

The search pipeline architecture from #1269 is largely implemented. The problem is not architecture — it is **quality at each layer** and **gaps in coverage**.

| Component | Status | Quality |
|-----------|--------|---------|
| BM25 inverted index (segmented, mmap-backed) | Implemented | Competitive (beats Pyserini BM25-flat 11/14 BEIR) |
| Tokenizer (UAX#29 + Porter + 33 stopwords) | Implemented | Basic — no phrase, no compound words |
| Hybrid (BM25 + MiniLM vectors + RRF) | Implemented | +12.8% avg over keyword-only |
| Query expansion (lex/vec/hyde via API) | Implemented | Untested on BEIR |
| Reranking (LLM-as-judge via API) | Implemented | Untested on BEIR |
| Position-aware blending | Implemented | Hand-tuned weights |
| Strong signal detection | Implemented | Threshold-based (0.85/0.15) |
| JSON/Event/State search | **Stubbed** | Returns empty |
| Graph integration | **Not started** | Per #1270 |
| Write-time enrichment | **Not started** | Highest latency/quality ROI |

### Identified Gaps

1. **Primitive coverage** — JSON, Event, State return empty from search despite being indexed on write
2. **Tokenization quality** — Porter stemmer only, strips special chars aggressively (loses "COVID-19", "C++"), fixed 33-word stopword list
3. **Scoring model** — Flat 20% title boost instead of principled BM25F field-aware scoring
4. **Snippet quality** — Fixed 100-char truncation from document start; no query-aware passage extraction
5. **Embedding model** — MiniLM-L6 only (384-dim); no model choice
6. **Reranker quality** — LLM-as-judge prompt, not a trained cross-encoder
7. **No write-time enrichment** — All intelligence applied at query time
8. **No graph integration** — Per #1270
9. **No in-process inference** — Expansion/reranking require external API endpoint

---

## Phase 1: Establish Baselines (BEIR)

**Goal**: Reproducible numbers before changing anything.

**Work**:
1. Run full BEIR suite — `keyword` mode (BM25 baseline)
2. Run full BEIR suite — `hybrid` mode (BM25 + MiniLM + RRF)
3. Run full BEIR suite — `hybrid-llm` mode (+ expansion + reranking)
4. Per-query nDCG@10 analysis on datasets where Strata underperforms
5. Publish baseline table in strata-eval with Pyserini/ColBERT/ELSER comparisons

**Why first**: Every subsequent phase is measured against this. Without it, we are optimizing blind.

**Effort**: Low — strata-eval already has the harness and partial results (14/15 datasets x 2 modes). Needs clean runs on current codebase.

---

## Phase 2: BM25 Foundation

**Goal**: Make the zero-dependency BM25 path as strong as possible. This is what every user gets without configuring models.

### 2a: Primitive Coverage (~300 lines)

The most glaring gap. JSON, Event, and State primitives **already index on write** (`search/recovery.rs` scans them, `index.rs` indexes their text) but **return empty from `search()`**. The inverted index has the data; the primitives just don't query it.

**Changes**:
- `primitives/json.rs`: Implement `Searchable::search()` — tokenize query, call `index.score_top_k()`, resolve doc_ids, fetch snippets from JSON values. Follow the exact pattern in `kv.rs` lines 336-406.
- `primitives/event.rs`: Same pattern — search indexed `"{event_type} {payload}"` text
- `primitives/state.rs`: Same pattern — search indexed `"{name} {value}"` text

**Impact**: High — users storing data in JSON/Event/State suddenly get search results. BEIR won't test this (BEIR uses KV) but real-world usage will.

### 2b: Tokenizer Improvements (~200 lines)

Current tokenizer strips all non-alphanumeric characters. This loses "COVID-19", "C++", version numbers, hyphenated terms.

**Changes** in `search/tokenizer.rs`:
- Preserve hyphenated compounds: "COVID-19" -> ["covid-19", "covid", "19"]
- Preserve alphanumeric sequences: "C++", "x86_64" kept as tokens
- Configurable stopword list (current 33 is small; add option for SMART 571-word list)
- Optional Snowball stemmer alongside Porter (more aggressive, better for some languages)
- Number normalization

**Impact**: Medium — affects recall on technical/medical corpora (NFCorpus, SciFact, TREC-COVID in BEIR).

### 2c: BM25F Field-Aware Scoring (~250 lines)

Currently: flat 20% title boost applied as a post-hoc multiplier. BM25F (Zaragoza et al., 2004) is the principled approach — per-field term frequencies, per-field length normalization, per-field weights.

**Changes** in `search/searchable.rs`:
- Extend `SearchDoc` with structured fields (title, body, metadata) instead of flat text
- `BM25FScorer` that computes per-field TF contributions with field weights
- Default weights: title=3.0, body=1.0, metadata=0.5 (tunable)
- Per-field length normalization (short titles shouldn't be penalized like long bodies)

**Changes** in `search/index.rs`:
- `PostingEntry` extended with field indicator (2 bits: title/body/meta)
- `index_document()` accepts structured fields, stores per-field TF
- `score_top_k()` passes field-level TF to scorer

**Impact**: Medium-High — BEIR corpora have title+text structure. BM25F is a standard win on datasets with meaningful titles (NFCorpus, FiQA, SciFact).

### 2d: Query-Aware Snippets (~150 lines)

Current: fixed 100-char truncation from start of document. This often misses the relevant passage entirely.

**Changes** in `search/searchable.rs`:
- `extract_best_snippet(text, query_terms, max_len)` — sliding window that maximizes query term density
- Return the window with the most query term matches
- Mark term positions for optional highlighting

**Impact**: Low on BEIR scores (snippets don't affect nDCG), but high on user-perceived quality and critical for reranker input quality (Phase 4).

### 2e: Proximity Scoring (~200 lines)

Documents where query terms appear near each other should score higher. "machine learning" appearing as adjacent words is more relevant than "machine" in paragraph 1 and "learning" in paragraph 20.

**Changes** in `search/searchable.rs`:
- Track term positions in `PostingEntry` (or a separate position index)
- `proximity_boost(positions_map, query_terms)` computes minimum span
- Multiply BM25 score by proximity factor: `1 + prox_weight * (1 / min_span)`

**Impact**: Medium — particularly on multi-word queries in BEIR (HotpotQA, FiQA).

**Measure after Phase 2**: Re-run BEIR keyword mode. Target: match or exceed Pyserini BM25-multifield on all 15 datasets.

---

## Phase 3: Non-LLM Hybrid Optimization

**Goal**: Best possible BM25+vector fusion without any LLM dependency.

### 3a: Fusion Parameter Sweep (~50 lines + eval scripts)

Current RRF uses k=60, original query weight=2.0, top-rank bonus +0.05/+0.02. These are reasonable defaults but not validated.

**Work**:
- Sweep k_rrf: [20, 40, 60, 80, 100] across BEIR datasets
- Sweep original weight: [1.0, 1.5, 2.0, 2.5, 3.0]
- Sweep top-rank bonus: [0, 0.02/0.01, 0.05/0.02, 0.10/0.05]
- Find per-dataset optimal and global best compromise
- Implement as configurable parameters (currently hardcoded)

**Impact**: Low-Medium — RRF is robust to parameter choice, but 1-2% nDCG gain is plausible.

### 3b: Embedding Model Support (~300 lines)

MiniLM-L6-v2 (384-dim, 22M params) is fast but not state-of-the-art for retrieval. Supporting model choice unlocks quality gains without architectural changes.

**Changes**:
- Abstract embedding model behind a trait (partially exists as `QueryEmbedder`)
- Support ONNX models beyond MiniLM: nomic-embed-text-v1.5, bge-small-en-v1.5, all-MiniLM-L12-v2
- Model selection via `StrataConfig` or environment variable
- Auto-download and cache models (extend existing model_dir() infrastructure)

**Impact**: High — embedding model quality directly affects hybrid search. nomic-embed scores ~5% higher than MiniLM-L6 on MTEB retrieval tasks.

### 3c: Learned Sparse Expansion — Write-Time (~500 lines)

**This is the highest-ROI non-LLM feature.** ELSER (Elastic) showed that expanding documents at write time with semantically related terms dramatically improves BM25 recall — with zero query-time cost.

**Approach**:
- At `index_document()` time, use the embedding model to find semantically similar terms
- Method: embed the document, find top-K nearest terms from a vocabulary embedding (pre-computed)
- Store expanded terms in the inverted index with a reduced weight factor (e.g., 0.3x normal TF)
- BM25 query matches on both original and expanded terms transparently

**Implementation**:
- `search/sparse_expansion.rs`: `expand_document(text, model, vocab_embeddings) -> Vec<(String, f32)>`
- Pre-compute vocabulary embeddings for top 50K terms from a reference corpus
- Ship as a `.vocab.bin` file alongside the embedding model
- `index.rs`: `PostingEntry` gains a `weight` field (1.0 for original, 0.3 for expanded)
- `score_top_k()` multiplies TF by weight

**Impact**: High — ELSER achieves +10-20% nDCG@10 over plain BM25 on BEIR. Our approach is simpler (no learned sparse encoder training) but captures most of the benefit.

**Trade-off**: Index size increases ~2-3x. Index time increases (one embedding per document). Query time is unchanged.

**Measure after Phase 3**: Re-run BEIR hybrid mode. Target: top-3 on BEIR leaderboard for embedded/local systems.

---

## Phase 4: LLM-Enhanced Pipeline

**Goal**: Maximum quality when an LLM is available (local or cloud).

### 4a: In-Process Inference (~200 lines)

Current expansion/reranking require an external OpenAI-compatible API endpoint. This adds latency, deployment complexity, and a failure mode.

**Changes**:
- New `InProcessExpander` implementing `QueryExpander` — calls `strata_intelligence::generate()` directly
- New `InProcessReranker` implementing `Reranker` — same
- Falls back to `ApiExpander`/`ApiReranker` if no local model loaded
- Feature-gated behind `intelligence` feature

**Impact**: Reduces expansion latency from ~500ms (HTTP) to ~200ms (in-process). Eliminates external dependency.

### 4b: Expansion Quality Improvements (~150 lines)

Current expansion prompt is generic. Improvements:

**Changes** in `expand/prompt.rs`:
- Include index statistics in prompt: "This database contains {N} documents about {top_terms}"
- Corpus-aware expansion: sample 3-5 high-IDF terms from the index to ground expansions
- Stricter drift guard: require >= 2 shared stems (not just 1 term)
- HyDE length calibration: match average document length in index

**Changes** in `expand/parser.rs`:
- Score expansions by estimated utility (IDF of expansion terms vs original)
- Drop low-utility expansions instead of using all of them
- Cache expansion results by query hash

**Impact**: Medium — better expansions = better recall without precision loss.

### 4c: Cross-Encoder Reranking (~400 lines)

Current reranker is an LLM prompted to score 0-10. A trained cross-encoder (ms-marco-MiniLM) is faster, more calibrated, and doesn't hallucinate scores.

**Changes**:
- `rerank/cross_encoder.rs`: Load ms-marco-MiniLM-L-6-v2 via ONNX
- `CrossEncoderReranker` implementing `Reranker` trait
- Input: (query, passage) pairs -> relevance logit
- Batch inference: all candidates in one forward pass
- Falls back to `ApiReranker` if no cross-encoder model available

**Impact**: High — cross-encoders consistently outperform prompted LLMs for relevance scoring. ms-marco-MiniLM achieves 0.39+ nDCG@10 on MS MARCO, close to ColBERT.

### 4d: Blending Weight Optimization (~100 lines + eval)

Current position-aware weights are hand-tuned. With BEIR baselines, we can optimize them.

**Work**:
- Grid search blending weights on BEIR dev splits
- Optimize for nDCG@10
- Validate on held-out splits (no overfitting)
- May reveal that optimal weights are dataset-dependent — consider a "confidence-aware" blending that uses score distributions rather than fixed positions

**Measure after Phase 4**: Full BEIR ablation matrix — each component on/off. This is the core of the research paper.

---

## Phase 5: Graph-Augmented Search

Per #1270, implemented incrementally. **Only start after Phases 1-4 are measured.**

### 5a: Graph Proximity Boost (~200 lines)
- Post-RRF score boost for graph-reachable hits
- Uses existing `nodes_for_entity()` reverse index + BFS traversal
- New SearchQuery fields: `graph_aware`, `anchor_nodes`, `max_hops`, `graph_weight`

### 5b: Graph-Context Reranking (~150 lines)
- Enrich reranker snippets with entity type + 1-hop neighbors before sending to cross-encoder/LLM

### 5c: Three-Signal Blending (~50 lines)
- Extend `blend_scores()` with graph proximity as a third signal
- Position-dependent weights for the graph signal

### 5d: Ontology-Guided Expansion (~300 lines)
- Include `ontology_summary()` in expansion prompt
- Graph neighborhood context for domain-specific expansions

### 5e: LLM Retrieval Planning (~500 lines)
- LLM reads ontology, generates structured retrieval plan (graph traversals + typed text searches)
- Opt-in only (`graph_aware: true`) due to latency

**Evaluation**: Custom graph-structured benchmark (BEIR has no graph data). Candidates: FinanceBench, medical knowledge graph + BioASQ.

---

## Phase 6: Write-Time Enrichment

**Only after query-time pipeline is proven on BEIR.**

### 6a: Auto-Entity Extraction
- On KV/JSON write, LLM extracts entities -> graph nodes
- Requires ontology to guide extraction
- Background task (non-blocking writes)

### 6b: Full Learned Sparse Expansion
- Train a sparse encoder on domain data (beyond nearest-vocab approach from Phase 3c)
- Requires training infrastructure

### 6c: Section-Hierarchy Indexing
- For long documents, extract section tree into graph
- Each section becomes a searchable unit with structural context

---

## Cross-Cutting Concerns

### Modular Pipeline

Every component is independently toggleable. The `SearchQuery` struct grows incrementally:

```
Phase 2: + primitives, mode (already exist)
Phase 3: + embedding_model, sparse_expansion
Phase 4: + expand, expand_types, rerank, rerank_model, rerank_candidates
Phase 5: + graph_aware, graph, graph_mode, anchor_nodes, max_hops, graph_weight
```

Each new field is `Option<T>` with an "auto" default that does the right thing based on what's configured. Agents and users choose their latency/quality tradeoff by enabling/disabling stages.

### Latency Budget

Every phase must track latency. The search handler already has budget enforcement (100ms default). Each layer gets a sub-budget:

| Layer | Target latency |
|-------|---------------|
| BM25 only | <10ms |
| + vector search | <50ms |
| + RRF fusion | <1ms |
| + expansion (in-process) | <300ms |
| + expansion (API) | <1000ms |
| + cross-encoder rerank | <100ms |
| + LLM rerank | <1000ms |
| + graph boost | <10ms |
| + graph planning | <2000ms |

### Graceful Degradation

Current principle: no search stage failure propagates to the caller. Every new component wraps in `try_*` and falls back. This invariant must be maintained across all phases.

### Per-Stage Stats Reporting

The `SearchStatsOutput` should report which stages ran and their individual latencies. Critical for ablation studies and for agents learning which stages help.

Extend `SearchStatsOutput`:
```rust
pub struct StageStats {
    pub name: String,           // "bm25", "vector", "rrf", "expand", "rerank", "graph_boost"
    pub enabled: bool,
    pub elapsed_ms: f64,
    pub candidates_in: usize,
    pub candidates_out: usize,
}
```

### BEIR-Gated Shipping

No phase ships without measured lift on BEIR. The strata-eval harness is the gate:

```
Phase N implemented -> Run BEIR suite -> nDCG@10 delta positive -> Ship
                                       -> nDCG@10 delta negative -> Investigate, fix, re-run
```

---

## Implementation Order

```
Phase 1  -->  Phase 2a,2c  -->  Phase 2b,2d,2e  -->  Phase 3a,3b  -->  Phase 3c
  |              (parallel)         (parallel)           (parallel)          |
  |                                                                         v
  |                                                                   Phase 4a,4c
  |                                                                    (parallel)
  |                                                                         |
  |                                                                         v
  |                                                                   Phase 4b,4d
  |                                                                         |
  +--- BEIR baseline --- BEIR re-measure --- BEIR re-measure --- BEIR ablation matrix
                                                                            |
                                                                            v
                                                                      Phase 5a-5c
                                                                            |
                                                                            v
                                                                      Phase 5d-5e
                                                                            |
                                                                            v
                                                                        Phase 6
```

---

## Priority Table

| Priority | Phase | Impact | Effort | LLM Required |
|----------|-------|--------|--------|--------------|
| 1 | 1: Measure baseline | Foundational | Low | No |
| 2 | 2a: Primitive coverage | High | Medium | No |
| 3 | 2c: BM25F field-aware scoring | Medium-High | Medium | No |
| 4 | 2b: Tokenizer improvements | Medium | Medium | No |
| 5 | 3a: Fusion parameter sweep | Medium | Low | No |
| 6 | 2d: Query-aware snippets | Medium | Low | No |
| 7 | 2e: Proximity scoring | Medium | Medium | No |
| 8 | 3b: Embedding model support | High | Medium | No |
| 9 | 4a: In-process inference | High | Medium | Yes |
| 10 | 4c: Cross-encoder reranking | High | Medium | Yes (model) |
| 11 | 3c: Write-time sparse expansion | High | High | No* |
| 12 | 4b: Improved expansion | Medium | Low | Yes |
| 13 | 4d: Blending optimization | Medium | Low | Yes |
| 14 | 5a-5c: Graph boost/context/blend | Medium | Medium | No/Yes |
| 15 | 5d-5e: Graph expansion/planning | High | High | Yes |
| 16 | 6: Write-time enrichment | High | High | Yes |

\* Write-time sparse expansion uses the embedding model at write time but not at query time.

---

## References

- #1269 — Hybrid search pipeline: query expansion, RRF fusion, reranking
- #1270 — RFC: Graph-Augmented Hybrid Search
- #1272 — Recursive Query Execution
- #1274 — Agent-First API Design
- [BEIR Benchmark](https://github.com/beir-cellar/beir) — Heterogeneous retrieval evaluation
- [Elasticsearch ELSER](https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-search-elser.html) — Learned sparse encoder
- [BM25F](https://dl.acm.org/doi/10.1145/1031171.1031181) — Zaragoza et al., 2004
- Reciprocal Rank Fusion — Cormack, Clarke, Buettcher, 2009
