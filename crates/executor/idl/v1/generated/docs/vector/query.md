---
title: "Query vectors"
description: "Search a vector collection."
source: strata-core@1.2.0
section: vector
---

Runs vector search through the engine planner and returns the best matches with scores and optional metadata.

Search responses return a bounded list of matches ordered by the engine. They are not cursor pages unless a later command explicitly advertises pagination.

## Examples

Find the nearest vectors to a query vector.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
$ strata vector upsert docs a [1.0,0.0,0.0]
$ strata vector upsert docs b [0.0,1.0,0.0]
$ strata vector query docs [1.0,0.0,0.0] --k 2
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"b","type":"vector_upsert","vector":[0.0,1.0,0.0]}
{"collection":"docs","k":2,"query":[1.0,0.0,0.0],"type":"vector_query"}
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

`SearchResult<VectorMatch>`.

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

- CLI: `strata vector query`
- Wire type: `vector_query`
