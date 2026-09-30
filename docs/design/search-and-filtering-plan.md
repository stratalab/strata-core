# Search and filtering in Strata

**Status:** draft for review (decisions D1–D6 open) · **Drafted:** 2026-09-30 against `main` at `87fa1a9c` (v1.2.7) · **Workstream:** 1.3–1.5

An engine-owned index you declare once, kept current in the same commit as your
data, and queried with small typed JSON filters. No query language.

## Summary

- **The gap.** Every vector query is an exact scan of the whole collection. JSON
  indexes are written but never read. There is no text search, no field filter on
  JSON, and no substring key search.
- **The model.** Redis-style index declarations on a key prefix with typed fields;
  Lucene-style analysis and BM25 for text; qmd-style fusion of keyword and vector
  results with reciprocal rank fusion (RRF). Index definitions and their state live
  in each branch's system space, so forks and time travel carry them.
- **Native inference.** Strata's own local inference turns qmd's model stages into
  product features: an index field can be auto-embedded, and a query can ask for
  expansion or reranking. The engine still never calls a model; it owns the rows
  that model output is stored in.
- **The order.** Index layer and structured filters first, because two apps and the
  VS Code extension are waiting on them. Then key search, text, production vector
  indexing, auto-embedding and hybrid ranking, then query-time model stages.
- **No format change.** Index entries are ordinary versioned rows, the way JSON
  index rows already are. Vector ANN artifacts keep their existing sidecar files.

## Where we are

What a user gets today, per capability. Facts come from three code surveys and a
full issue inventory run on 2026-09-30.

| Capability | State | What actually happens |
|---|---|---|
| Vector query | scan only | Every query scans and scores the whole collection. HNSW and flat index code exists (`crates/engine/src/data/vector/index.rs`), but sealing (`seal_index_artifacts_from_snapshot`) is reachable only from tests, and `DEFAULT_SOURCE_HNSW_THRESHOLD` is `usize::MAX`. Historical (`as_of`) reads always scan. |
| Vector filter | minimal | AND of equality on top-level metadata fields, applied after scoring. No range, `in`, OR or nested fields (#1890, #3121). |
| JSON indexes | write-only | Index rows are written in the same commit as each document (`index_mutations_for_change`), so they are correct across branches, forks and time travel. No query reads them (#2703). Branch promotion does not carry them (#3728). |
| JSON queries | none | `json.list`, `scan`, `count`, `sample` take a key prefix only. No field predicate or projection. |
| KV keys | prefix | Prefix, cursor, limit and `as_of` paging works (`KvService::list_page`, `list_at_page`). No glob, substring or fuzzy match (#3017). |
| Text search | none | No tokenizer, inverted index or BM25 in the V1 engine. `strata search` refuses. The pre-V1 BM25 code is gone. |
| Events | scan | A type-index row is written per append but only serves `list_types`. Reads by type or time scan every event. |
| Graph | type only | `nodes_by_type` uses a real index. No property filters or property indexes (#1891). |
| Contract doc | stale | `docs/architecture/engine/retrieval-and-derived-state-contract.md` cites `crates/engine/src/search` and `crates/intelligence`, neither of which exists. |

The useful foundation: JSON index rows, graph type-index rows and event type-index
rows are all written in the same commit as their source row. That one pattern
already gives correctness across branches, copy-on-write forks, `as_of` reads and
deletes. The plan builds on it.

## Who is waiting

- **VS Code extension:** a Redis-style key browser with a live filter (#3017) and
  hub dataset search (#3041). Prefix browsing works today; substring does not.
- **Island app:** exact and compound lookups on JSON fields (#3482) and
  autocomplete (#3483). Its graph asks (#3484 weighted paths, #3561 bounding box)
  are separate 1.3 graph work.
- **Python SDK:** the inert index API (py#60), index method naming (py#77), and
  unfilterable non-dict metadata (#2705).
- **Agents:** the vector `--filter` shape is undiscoverable and its error names a
  serde internal (#3121).

## What we borrow

### Redis (Query Engine / RediSearch)

- **Take:** `FT.CREATE … ON JSON PREFIX … SCHEMA`. One declaration names a key
  prefix and typed fields, and every later write keeps the index current. The field
  type decides the allowed operations (TAG exact and in, NUMERIC ranges, TEXT
  full-text, VECTOR nearest neighbour). Existing data is indexed in the background
  with visible progress (`FT.INFO percent_indexed`). `SCAN … MATCH <glob>` for
  cursor-paged key matching with no index at all.
- **Skip:** the query string (`@tags:{a|b} @age:[20 30]`). It is a language with
  its own escaping and parser. We keep the model and drop the syntax.

### Lucene and Elasticsearch

- **Take:** analyzers (a deterministic tokenizer plus lowercase folding, applied the
  same way at write and query time); inverted postings and BM25 (k1 = 1.2,
  b = 0.75); the `keyword` versus `text` field split, which is our tag versus text;
  filter context versus query context (filters narrow the set without affecting
  score, queries rank what is left); `search_after`-style cursors so paging stays
  stable while writes continue.
- **Skip:** the open-ended bool query tree, scripting, and per-index mapping sprawl.
  We use the clause shape and keep it flat.

### qmd (Tobi Lütke)

- **Take:** run keyword (BM25) and vector retrieval side by side and merge them with
  reciprocal rank fusion, `score = Σ 1 / (60 + rank)`. RRF needs no score
  calibration and is deterministic, so it belongs in the engine. Optional query
  expansion with a small local model before retrieval, and optional reranking of the
  fused top results with a local reranker, both running on the user's machine.
- **Skip:** putting model calls on the default path. Expansion and reranking are
  opt-in and live outside the engine: the engine never calls a model provider
  (CLAUDE.md hard rule 23).

## The design

### 1. Declare an index on a prefix

One command creates an index over the documents under a key prefix in a space. Each
field has a path, a name and a type. The declaration is stored on the branch, so
forks inherit it.

```json
index.create {
  "space":  "places",
  "name":   "places_idx",
  "source": "json",
  "prefix": "place:",
  "fields": [
    { "path": "$.name", "as": "name", "type": "text" },
    { "path": "$.kind", "as": "kind", "type": "tag" },
    { "path": "$.pop",  "as": "pop",  "type": "numeric", "sortable": true }
  ]
}
```

Field types, and the only operations each allows:

| Type | Stored as | Filter ops | Scored op |
|---|---|---|---|
| tag | exact value, case kept | `eq`, `in`, `not_in` | none |
| numeric | order-preserving f64 bytes | `eq`, `gt`, `gte`, `lt`, `lte` | none (sortable) |
| text | analyzed terms, postings | `prefix` | `match` (BM25) |
| vector | collection embedding, or auto-embedded | none | `near` (k nearest) |

Today's tag index lowercases values and silently rewrites paths. The new tag type
keeps values exact and rejects paths it cannot index, with a typed error.

### 2. Where the metadata lives

Every database has a system branch, and every branch has a system space. That gives
each piece of search state an obvious home.

| State | Home | Why there |
|---|---|---|
| Index definitions | the branch's system space | A fork inherits its parent's indexes by copy-on-write, and `as_of` sees the definitions that existed at that version. |
| Backfill progress, freshness | the branch's system space | What `index.info` reports; per branch, because a fork's backfill is its own. |
| Vector index manifests | the branch's system space | Today they are control rows in the user space; moving them keeps all derived-state bookkeeping in one place. |
| Auto-embedded vectors | the branch's system space | Derived rows (the reserved `0x41` shadow family), separate from vectors a user writes, and rebuildable from source. |
| Model registry, analyzer presets | the system branch | Shared by every branch; a model downloaded once serves them all. |

Index entries themselves (tag, numeric and text postings) stay beside the data they
index, written in the same commit, so they need no separate bookkeeping.

### 3. Keep it current in the same commit

- **Every write** to a document under the prefix writes its index entries in the
  same commit, as JSON index rows do today. Branches, forks, `as_of` reads and
  deletes stay correct with no extra machinery.
- **Creating an index on existing data** backfills in bounded commits, the same
  mark-then-sweep pattern space deletion uses (#3574). `index.info` reports
  progress. Until the backfill finishes, queries fall back to a validated scan and
  say so in their stats.
- **Promotion** re-derives index entries for every promoted change (#3728), through
  the unused `DerivedDisposition::Rebuildable` hook.
- **Source validation:** every candidate the index returns is re-checked against its
  source row at the read's version before it is returned. The index accelerates
  reads; it never decides the answer (hard rule 26).

### 4. Query with typed JSON, not a language

One `search` command. `filter` clauses are ANDed and never scored; `match` and
`near` are scored. Alternatives within one field use `in`.

```json
search {
  "index":  "places_idx",
  "filter": [
    { "field": "kind", "in":  ["station", "park"] },
    { "field": "pop",  "gte": 1000, "lt": 50000 }
  ],
  "match":  { "field": "name", "text": "grand central" },
  "sort":   { "field": "pop", "order": "desc" },
  "limit":  20,
  "cursor": null,
  "as_of":  null
}
```

Response:

```json
{
  "hits":   [ { "key": "place:1042", "score": 7.31, "fields": { "name": "Grand Central", "kind": "station" } } ],
  "cursor": "…",
  "stats":  { "used_index": true, "rows_examined": 41, "exact": true, "fusion": null }
}
```

The clause schema lives in the IDL like every other command, so the CLI, the Python
SDK and MCP get it from one definition, and an agent sees the valid operations for
each field type in the schema instead of learning a syntax.

### 5. Keys: glob first, index later

`kv list --match 'In*'` follows Redis `SCAN MATCH`. A pattern with a literal prefix
becomes a range scan; anything else scans the space and reports `rows_examined`, so
the cost is never hidden. Substring and fuzzy search over millions of keys then use
a trigram index on key names, built on the same index layer (#3017, #3483).

### 6. Text: deterministic analysis and BM25

A fixed analyzer (Unicode word boundaries, lowercase, ASCII folding; no stemming at
first) produces terms. Postings are index rows keyed by field, term and document.
BM25 needs corpus statistics: document count, average field length and document
frequency per term.

**Design risk.** The statistics belong in the system space, but keeping them as rows
updated on every commit makes them a write hotspot and a source of conflicts between
concurrent writers, wherever they live. The alternative is counting postings at query
time, which costs reads on common terms. S4 starts with a short design note on this
before any code.

### 7. Vectors: make the index real

- A maintenance task seals the unindexed delta into flat and HNSW artifacts past a
  threshold near `vector-indexing-design.md`'s 2–4k vectors, replacing the
  test-only seal.
- `as_of` queries get an indexed path within the fork and version caps the manifest
  already carries.
- Vector collections join `search` as a `near` clause, and their metadata gets the
  same typed filters. A selective filter pre-filters through the field index; a
  broad one uses HNSW with post-filtering and the existing exact fallback.
  ACORN-style filtered traversal (#1581) is a later optimisation.
- A latency perf gate on query time against collection size, which does not exist
  today.

### 8. Hybrid ranking

When a query has both `match` and `near`, the engine fuses the two ranked lists with
RRF (k = 60) and reports it in `stats.fusion`. That is deterministic and model-free.

### 9. Native inference: embedding, expansion, reranking

Strata already runs models locally through `strata-inference`, and
`vector.upsert` / `vector.query` already accept text and embed it with the
collection's recorded model. The search work builds on that instead of a separate
service.

- **Auto-embedding as a field type.** A text field can be declared
  `"type": "vector", "embed": { "model": "nomic-embed" }`. Every write to a matching
  document queues its embedding; the vectors land as derived rows in the system
  space. Embedding runs in the background, not inside the write, and `index.info`
  reports how far behind it is. Until a document is embedded it is still found by
  filters and `match`, just not by `near`.
- **Model identity is part of the index.** The index records the embedding model;
  querying with a different one fails with
  `failed_precondition.engine.embedding_model_mismatch` (hard rule 24), and changing
  the model is a rebuild, reported like a backfill.
- **Query expansion.** `"expand": { "model": "…" }` rewrites the query into a few
  variants with a small local model before retrieval; their results are fused with
  RRF, as qmd does.
- **Reranking.** `"rerank": { "model": "…", "top": 30 }` reorders the fused top
  results with a local reranker. `stats` reports which stages ran and how long each
  took. The model catalog has embedding models (`miniLM`, `nomic-embed`, `bge-m3`,
  `gemma-embed`) and generators (`qwen3:1.7b` and others) but no reranker yet
  (#3380), so adding a GGUF reranker such as BGE-reranker-v2-m3 to the catalog is a
  prerequisite for S8. `bge-reranker` in the example below is that planned entry.

```json
search {
  "index":  "notes_idx",
  "filter": [ { "field": "project", "eq": "island" } ],
  "match":  { "field": "body", "text": "tunnel ventilation" },
  "near":   { "field": "body_vec", "text": "tunnel ventilation", "k": 50 },
  "expand": { "model": "qwen3:1.7b" },
  "rerank": { "model": "bge-reranker", "top": 30 },
  "limit":  10
}
```

All of this sits above the engine: the engine stores the vectors, runs retrieval and
fuses the lists, and the layer that calls the model decides what to embed and how to
rerank (hard rules 23 and 25). Every model stage is opt-in per index or per query, so
a search without them is fully deterministic.

## Slices

Each slice is one PR or a short series, with implementation and tests together, under
about 1,500 lines, with no durable-format change. Every slice that adds a command
goes through the IDL and reaches the CLI, the Python SDK and MCP in the same slice.

### 1.3 — filters and keys (unblocks the waiting consumers)

- **S0. Design note and IDL shapes.** The index declaration and filter clause
  schemas in the IDL. Retire `json.index.*` (already marked transitional) in favour
  of `index.*`. Rewrite the stale derived-state contract doc to match the code.
  Prepares #2703, py#77.
- **S1. Index layer for tag and numeric fields.** Declarations in the system space,
  same-commit maintenance for JSON prefixes, bounded backfill with `index.info`
  progress, promotion re-derivation, space-delete sweep. Tests across forks,
  `as_of`, deletes and promotion. Closes #3728.
- **S2. `search` with filters, sort and cursors.** Typed filters, sort on sortable
  numeric fields, stable cursors, `as_of`, source validation. A work-bound test:
  rows examined grow with the page, not the index. Closes #2703, #3482, py#60.
- **S3. Key matching.** `kv list --match` glob with honest cost stats; a prefix
  autocomplete surface; a trigram key index for substring search. Closes #3017,
  #3483.

### 1.4 — ranking: text, real vector indexes, auto-embedding, hybrid

- **S4. Text fields and BM25.** The corpus-statistics design note, then the
  analyzer, postings rows, `match` and `prefix` on text fields.
- **S5. Production vector indexing.** Auto-seal, a realistic HNSW threshold, the
  `as_of` path, and a latency-versus-size perf gate.
- **S6. Vectors and auto-embedding in `search`.** `near` clauses; metadata fields as
  tag and numeric with selective pre-filtering; auto-embedded vector fields through
  native inference, embedded in the background with freshness in `index.info`;
  model identity checked per index. Closes #1890, #3121, #2705.
- **S7. Hybrid ranking with RRF.** Fuse `match` and `near`; explain the fusion in
  stats.

### 1.5 — query-time model stages

- **S8. Query expansion and reranking.** The `expand` and `rerank` options on
  `search`, running on local models through native inference, with per-stage
  timings in stats. Evaluate with a small labelled query set before turning either
  on by default anywhere.

## Decisions (open)

- **D1. One `search` command, or search per data type?** Recommend one command over
  declared indexes. Hard rule 8 wants one canonical path, and `json.search`,
  `vector.search` and so on would each grow their own filter shape. `vector.query`
  stays as the unindexed collection call.
- **D2. OR across fields.** Recommend only `in` within a field for 1.3. Cross-field
  OR is what turns a filter list into a tree; add an `any` group later only if a
  real query needs it.
- **D3. Retire `json.index.*`.** Recommend a clean break to `index.*` in S0. The old
  commands never did anything a user could observe, and they are already marked
  transitional.
- **D4. What an index can cover.** Recommend JSON prefixes in 1.3, vector
  collections in 1.4, and key names through the key index. Event payloads and graph
  properties later, on the same layer.
- **D5. Which layer calls the model.** Auto-embedding lands in 1.4, so this is needed
  by S6. Today the executor reaches inference behind a feature flag, and the
  intelligence layer designed for this (roadmap M8) was deferred. Recommend reviving
  it as a small search-only crate (embedding jobs, expansion, rerank) that uses
  engine surfaces and inference, as the architecture intended. The alternative,
  growing this in the executor, breaks the rule that the executor stays a thin
  adapter.
- **D6. Release split.** Recommend 1.3 = S0–S3, 1.4 = S4–S7 (including
  auto-embedding), 1.5 = S8. Filters and key search unblock three consumers without
  waiting on BM25, vector indexing or models.

## Out of scope

- A query string language of any kind, or scripting.
- Model calls inside the engine, or on any search that does not ask for them.
- Aggregations and facets beyond what a sort and a filter give (#2296, #2297). Tag
  counts may follow once the index layer exists.
- Geo fields. The island app's bounding-box ask (#3561) is 1.3 graph work.
- Any durable-format change. Index entries are rows; vector artifacts keep their
  sidecar files.

## Issue cleanup (after approval)

The inventory found about 90 open search-related issues, most filed before the V1
promotion against code that no longer exists.

- **Close as obsolete:** the pre-V1 `Searchable` series (#1872–#1876, #1936, #1949),
  #1322, #1484, #1889 and py#12 (cursors shipped), py#11 (bundles removed).
- **Fold into one new V1 epic:** the #2106 umbrella and its duplicates: ACORN
  (#1581, #2264, #2293, #2295), full-text operators (#1885 and children), BM25F
  (#2250, #2272), graph property indexes (#1891, #2206).
- **Park with the model stages:** reranking, expansion, RAG and recipe-tuning issues
  (#1636, #2267, #2269–#2274, #2300–#2313).
