# Strata Retrieval Architecture

## Documents

| Document | Purpose |
|----------|---------|
| [`search-strategy.md`](search-strategy.md) | **Start here.** Philosophy, three-layer architecture, ablation study, implementation roadmap. |
| [`retrieval-substrate.md`](retrieval-substrate.md) | Layer 1: Recipe schema, pipeline, invariants, operators, predicates, aggregations. |
| [`intelligence-layer.md`](intelligence-layer.md) | Layer 2: Query-time flow, write-time enrichment, RAG, AutoResearch, Qwen3 usage. |
| [`v0.0-prerequisites.md`](v0.0-prerequisites.md) | Prerequisites: _system_ space, recipe types, config migration, primitive fixes. |
| [`v0.1-implementation.md`](v0.1-implementation.md) | Implementation plan: minimal substrate (BM25 + vector + RRF + limit). |
| [`v0.2-implementation.md`](v0.2-implementation.md) | Implementation plan: query expansion + reranking (qmd parity). |

## Related

| Document | Purpose |
|----------|---------|
| [`../search-pipeline-implementation-plan.md`](../search-pipeline-implementation-plan.md) | Phase-by-phase search quality improvements |
| [`../autoresearch-search-optimization.md`](../autoresearch-search-optimization.md) | Branch-parallel experimentation methodology |
