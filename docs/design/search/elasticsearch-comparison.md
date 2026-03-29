# Elasticsearch Comparison: What Strata Covers, What's Missing, What's Unnecessary

Comprehensive mapping of Elasticsearch's search knobs against Strata's recipe schema. Organized by: covered, planned, future, and deliberately excluded.

---

## 1. Covered by Strata's Recipe

These ES features have direct equivalents in `retrieval-substrate.md`.

| ES Feature | ES Default | Strata Recipe Path | Strata Default | Notes |
|---|---|---|---|---|
| **BM25 k1** | 1.2 | `retrieve.bm25.k1` | 0.9 | Strata uses Anserini/Pyserini BEIR-optimized defaults |
| **BM25 b** | 0.75 | `retrieve.bm25.b` | 0.4 | Same |
| **Per-field weights** | via multi_match boost | `retrieve.bm25.field_weights` | `{"body": 1.0}` | ES uses `^` syntax; Strata uses map |
| **Stemmer** | via analyzer chain | `retrieve.bm25.stemmer` | `"porter"` | ES has 40+ language stemmers; Strata has porter/snowball/none |
| **Stopwords** | via analyzer chain | `retrieve.bm25.stopwords` | `"lucene33"` | ES has language-specific lists; Strata has lucene33/smart571/none |
| **Phrase matching** | match_phrase, slop | `retrieve.bm25.phrase_boost` | 0.0 | ES has full phrase queries; Strata has boost multiplier |
| **Proximity scoring** | span_near | `retrieve.bm25.proximity_boost` | 0.0 | ES has full span queries; Strata has boost multiplier |
| **kNN search** | knn query | `retrieve.vector` | k=50, ef_search=100 | ES: num_candidates. Strata: ef_search (same concept). |
| **kNN + filter** | knn.filter | `filter.predicates` | Post-retrieval | ES supports pre-filter in kNN; Strata does post-filter |
| **Rescoring** | rescore query | `rerank` section | Disabled | ES: query rescore. Strata: cross-encoder rerank. |
| **from/size** | from=0, size=10 | `transform.offset`, `transform.limit` | 0, 10 | Direct mapping |
| **timeout** | No timeout | `control.budget_ms` | 5000 | ES can return partial results; Strata same (budget_exhausted flag) |
| **_source filtering** | All fields | `control.include_metadata`, `control.include_snippets` | true, true | Different granularity — ES per-field, Strata per-category |
| **Bool must/should/filter** | N/A | `filter.predicates` + `filter.logic` | "and" | ES: full bool query DSL. Strata: and/or predicate lists. |
| **Range query** | gt/gte/lt/lte | `filter.predicates` with range ops | N/A | Direct mapping of operators |
| **Term query** | Exact match | `filter.predicates` with eq | N/A | Same semantics |
| **Terms query** | Set membership | `filter.predicates` with in | N/A | ES: terms. Strata: in/not_in. |
| **Exists query** | Field exists | `filter.predicates` with exists | N/A | Direct mapping |
| **Prefix query** | String prefix | `filter.predicates` with prefix | N/A | Direct mapping |
| **Score threshold** | min_score in function_score | `filter.score_threshold` | None | Strata: simple threshold. ES: via function_score.min_score. |
| **Field collapsing** | collapse.field | `transform.deduplicate` | entity_ref | Similar concept — Strata deduplicates by entity ref |
| **RRF fusion** | rrf retriever | `fusion` section | RRF k=60 | ES 8.14+ has native RRF. Same algorithm. |
| **Query expansion** | Via synonyms/LLM | `expansion` section | Disabled | ES: static synonym files or ELSER. Strata: runtime LLM expansion. |

---

## 2. Planned (reserved in recipe schema, not yet implemented)

| ES Feature | Strata Recipe Path | Version |
|---|---|---|
| **Aggregations** (terms, histogram, stats, etc.) | `transform.aggregate` | Reserved for DataFusion |
| **Group by** | `transform.group_by` | Reserved for DataFusion |
| **Sort by field** | `transform.sort` | Reserved for DataFusion |
| **Scan** (predicate-only retrieval) | `retrieve.scan` | Reserved for DataFusion |
| **Sparse vector / ELSER** | `retrieve.sparse` | Reserved (no implementation date) |

---

## 3. Future Consideration (not in recipe, could be added)

| ES Feature | What it does | Priority | How Strata would do it |
|---|---|---|---|
| **Fuzzy matching** | Levenshtein distance matching | Medium | `retrieve.bm25.fuzzy` with max_edit_distance. Needs inverted index support. (#1885) |
| **Synonyms** | Map equivalent terms | Medium | `retrieve.bm25.synonyms` — list of synonym pairs, applied at tokenization. |
| **Highlighting** | Mark matched terms in snippets | Medium | `control.highlight: true` — annotate snippet with match positions. |
| **search_after cursor** | Efficient deep pagination | Medium | Better than offset for page 100+. Add `transform.search_after`. |
| **match_phrase with slop** | Phrase with gaps | Low | Already partially covered by phrase_boost + proximity_boost. Full slop would need position index. |
| **Boosting query** | Positive + negative boost | Low | Could be `filter.negative_boost` or a fusion weight modifier. |
| **Decay functions** (gauss, exp, linear) | Score by distance from origin | Low | Useful for geo/date. `fusion.decay` or a boost modifier. |
| **Suggesters** | Autocomplete / "did you mean" | Low | UX feature, not core retrieval. Could be intelligence layer. |
| **Nested queries** | Query relationships | Low | Strata's graph primitive covers this differently. |
| **Percolator** | Reverse search (match queries to docs) | Low | Niche. Could be a background alert system. |

---

## 4. Deliberately Excluded (Strata doesn't need these)

| ES Feature | Why Strata doesn't need it |
|---|---|
| **Cluster settings** | Strata is embedded, single node. No clusters. |
| **Shard routing / preference** | No shards. |
| **Cross-cluster search** | No clusters. |
| **Scroll API** | Use offset/limit. Strata datasets fit in memory. |
| **Shard request cache** | MVCC snapshot handles consistency. No cache invalidation problem. |
| **Node-level query cache** | Embedded — no network latency to optimize with caching. |
| **Index lifecycle management** | Strata has segments + compaction, not index rotation. |
| **Ingest pipelines** | Strata has write-time enrichment (intelligence layer). |
| **Watcher / alerting** | Out of scope for an embedded DB. |
| **Painless scripting** | No custom scoring scripts. BM25 + fusion weights cover it. |
| **DFR/IB/LM similarity models** | BM25 is sufficient. Alternative models add complexity with marginal gain. AutoResearch can tune BM25 parameters instead. |
| **Geo queries** | No geo data type in Strata. |
| **Join queries (parent/child)** | Strata's graph primitive handles relationships natively. |
| **Index templates** | Strata uses recipes, not index templates. |
| **Mapping management** | Strata's primitives are schemaless. |
| **Pipeline aggregations** | Query engine territory. DataFusion would handle this. |
| **Significant terms/text** | Niche aggregation. DataFusion territory. |

---

## 5. Where Strata Has More Than Elasticsearch

| Strata Feature | ES Equivalent |
|---|---|
| **Graph retrieval (PPR)** | None. ES has no graph traversal in search. |
| **Temporal search (as_of, diff)** | None. ES has no MVCC time-travel. |
| **Per-hit version history** | None. ES doesn't track document versions for search. |
| **AutoResearch (db.experiment)** | None. ES has no automated recipe optimization. |
| **Multi-model routing** | None. ES ELSER is one model, hardcoded. |
| **RAG built-in** | None. ES returns hits, no answer generation. |
| **Branch-scoped recipes** | None. ES index settings are global. |
| **Recipe three-level merge** | Partial. ES has index templates with priority, but not per-query overrides merged with stored defaults. |
| **In-process LLM** | None. ES ELSER runs in the cluster but isn't user-configurable per query. |

---

## 6. Strata Recipe Defaults vs. Elasticsearch Defaults

| Parameter | Strata | Elasticsearch | Why different |
|---|---|---|---|
| BM25 k1 | **0.9** | 1.2 | Strata uses Anserini/Pyserini BEIR-optimized values |
| BM25 b | **0.4** | 0.75 | Same — BEIR research shows lower b is better for mixed-length corpora |
| RRF k | **60** | 60 | Same |
| Result limit | **10** | 10 | Same |
| Budget/timeout | **5000ms** | None | ES has no default timeout; Strata enforces by default |
| Stemmer | **Porter** | Language-specific | ES auto-detects; Strata defaults to English Porter |
| Stopwords | **Lucene 33** | Language-specific | Same philosophy, different set |
| Vector ef_search | **100** | num_candidates auto | Strata exposes directly; ES abstracts behind num_candidates |

---

## 7. Key Architectural Differences

| Concern | Elasticsearch | Strata |
|---|---|---|
| **Configuration model** | Per-index settings + per-query DSL | Recipe (per-branch, three-level merge, experimentable) |
| **Analysis chain** | Index-time + search-time analyzers, configurable per field | Recipe-level stemmer/stopwords applied uniformly |
| **Scoring** | Pluggable similarity (BM25, DFR, IB, LM, scripted) | BM25 only. Tune k1/b via AutoResearch instead of switching models. |
| **Aggregations** | Built-in, 30+ types | Deferred to DataFusion. Search ≠ query engine. |
| **Vector search** | kNN query with pre-filter + post-filter | HNSW with post-filter (adaptive over-fetch) |
| **Reranking** | Query rescore (re-run a second query on top-N) | Cross-encoder / LLM reranking via intelligence layer |
| **Expansion** | Static synonyms or ELSER sparse | Runtime LLM expansion (lex/vec/hyde) with grammar constraints |
| **Experimentation** | Manual A/B testing of index settings | `db.experiment()` — branch-parallel recipe comparison |
| **Graph** | None native (requires external graph DB) | Built-in PPR, typed traversal, ontology-guided |
| **Temporal** | None (point-in-time API for scroll consistency only) | Full MVCC time-travel, diff, per-hit version history |
