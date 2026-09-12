---
title: "Query vector index"
description: "Search vectors and return index diagnostics."
source: strata-core@1.2.1
section: vector
---

Runs vector search and includes planner diagnostics such as index policy, source usage, artifact status, and fallback facts.

Search responses return a bounded list of matches ordered by the engine. They are not cursor pages unless a later command explicitly advertises pagination.

Diagnostic responses include operational facts intended for debugging and tuning. They should not be required for application correctness.

## Examples

Nearest-neighbor search that also returns index diagnostics.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a [1.0,0.0,0.0]
created a in docs
$ strata vector upsert docs b [0.0,1.0,0.0]
created b in docs
$ strata command run --command-json '{"collection":"docs","k":2,"query":[1.0,0.0,0.0],"type":"vector_index_query"}'
KEY  SCORE  METADATA
a      1.0  -
b      0.0  -

diagnostics
  active_delta_count            0
  active_delta_seal_threshold   16
  active_delta_source_count     0
  artifact_sources              -
  collection                    docs
  collection_exact_threshold    64
  derived                       0 B
  exact_fallback_count          0
  exact_source_count            1
  filtered_underfill_fallback   true
  flat_source_count             0
  hnsw_graph_builds             0
  hnsw_memory_budget            67.1 MB
  hnsw_source_count             0
  indexed_source_count          0
  indexed_vector_count          0
  last_query_fallback_reason    collection_below_exact_threshold
  last_query_used_index         false
  manifest_generation           -
  manifest_inherited_ref_count  0
  manifest_owned_ref_count      0
  manifest_ref_count            0
  manifest_status               missing
  overfetch_factor              4
  policy_mode                   auto
  resolved_index_kind_summary   exact
  source_candidate_limit        18446744073709551615
  source_flat_threshold         64
  source_hnsw_threshold         18446744073709551615
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"b","type":"vector_upsert","vector":[0.0,1.0,0.0]}
{"collection":"docs","k":2,"query":[1.0,0.0,0.0],"type":"vector_index_query"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `collection` | `string` | yes | Collection name. |
| `filter` | `VectorMetadataFilter` | no | Optional metadata filter. |
| `k` | `integer` | yes | Maximum number of matches. |
| `query` | `number[]` | yes | Query embedding. Accepted at wire (f64) precision and narrowed to the searched f32; a value that underflows or overflows f32 is rejected. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`SearchResult<VectorMatch> + IndexDiagnostics`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_filter`](https://stratadb.org/e/invalid_argument.engine.vector_filter)
- [`invalid_argument.executor.vector_limit`](https://stratadb.org/e/invalid_argument.executor.vector_limit)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `vector_index_query`
