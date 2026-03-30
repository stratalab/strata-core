# BEIR Baseline — v0.0 Search Quality

**Date:** 2026-03-30
**Harness:** `strata-benchmarks/src/bin/beir.rs` (Rust-native, calls executor directly)
**Model:** all-MiniLM-L6-v2 (384-dim, GGUF F16, GPU-accelerated via llama.cpp)
**BM25 params:** k1=0.9, b=0.4 (Strata defaults)
**Fusion:** RRF with k=60 (hybrid mode)

## Results

| Dataset   | Docs    | Queries | Keyword nDCG@10 | Hybrid nDCG@10 | Pyserini BM25 | Keyword Delta | Hybrid Delta |
|-----------|---------|---------|-----------------|----------------|---------------|---------------|--------------|
| nfcorpus  | 3,633   | 323     | 0.3183          | 0.3451         | 0.3218        | -0.0035       | +0.0233      |
| scifact   | 5,183   | 300     | 0.6709          | 0.7119         | 0.6647        | +0.0062       | +0.0472      |
| arguana   | 8,674   | 1,406   | 0.4030          | 0.4859         | 0.3970        | +0.0060       | +0.0889      |
| scidocs   | 25,657  | 1,000   | 0.1498          | 0.1972         | 0.1490        | +0.0008       | +0.0482      |

## Throughput

| Dataset   | Keyword QPS | Hybrid QPS | Index Time (keyword) | Index Time (hybrid) |
|-----------|-------------|------------|----------------------|---------------------|
| nfcorpus  | 62,013      | 816        | 0.4s                 | 10.2s               |
| scifact   | 21,723      | 348        | 0.6s                 | 14.4s               |
| arguana   | 2,505       | 202        | 0.7s                 | 18.2s               |
| scidocs   | 5,725       | 348        | 2.2s                 | 57.9s               |

## Analysis

**Keyword search** matches or slightly exceeds Pyserini BM25 on all four datasets. The nfcorpus delta (-0.0035) is within noise; the remaining three are positive. This validates Strata's BM25 implementation at k1=0.9, b=0.4.

**Hybrid search** beats BM25 on every dataset. The largest lift is on ArguAna (+0.0889), where queries are paraphrased counter-arguments to corpus documents — semantic similarity captures paraphrase overlap that keyword matching misses. SciFact and SciDocs also show meaningful gains from the embedding signal.

**Throughput** is dominated by embedding inference in hybrid mode. Keyword-only QPS ranges from 2.5K to 62K. Hybrid QPS is 200-800, bottlenecked by MiniLM inference at query time.

## Methodology

- Corpus indexed via `Strata::kv_put()` with title + text concatenation
- Hybrid mode enables `auto_embed`, which triggers background embedding via MiniLM
- Evaluation follows BEIR convention: `ignore_identical_ids` (filter out hits where doc_id == query_id before computing nDCG). This is critical for ArguAna where queries are corpus documents.
- Only queries with relevance judgments (qrels) are evaluated
- nDCG@10 computed per-query then averaged
- Pyserini baselines from the BEIR leaderboard (Thakur et al., 2021)

## Reproducing

```bash
cd strata-benchmarks
cargo run --release --bin beir -- --datasets nfcorpus,scifact,arguana,scidocs --mode keyword
cargo run --release --bin beir -- --datasets nfcorpus,scifact,arguana,scidocs --mode hybrid
```

Datasets are auto-downloaded from the BEIR repository on first run.
