---
title: "Query vectors"
description: "Search a vector collection."
source: strata-core@1.2.1
section: vector
---

Runs vector search through the engine planner and returns the best matches with scores and optional metadata. With `text` instead of a query vector, the text is embedded through the collection's recorded model on the same inference path as `inference.embed`, so the `inference.*` failures listed below apply only to that form; a supplied vector never reaches the inference runtime.

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
| `query` | `number[]` | no | Query embedding. Accepted at wire (f64) precision and narrowed to the searched f32; a value that underflows or overflows f32 is rejected. Empty when `text` is supplied instead. A vector carries no model, so when the collection records an embedding model, Strata cannot check this query against it: supplying one is the caller's statement that the recorded model produced it, and a query from another model returns neighbours that are ranked and meaningless. Only `text` is embedded under the record. |
| `text` | `string` | no | Text to embed with the collection's recorded model, instead of supplying a query vector (D10). This is the half that makes provenance worth recording: the query is embedded with the same model the collection was written with, so a caller cannot accidentally compare vectors from two models. With `as_of` or `as_of_time`, the model is the one the collection recorded at that snapshot. A snapshot older than the model's declaration is refused with `failed_precondition.engine.embedding_model_missing`: the declaration vouched for the vectors present when it was made, not for what the collection held before. Search such a snapshot with a `query` vector. |

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
- [`invalid_argument.executor.vector_input`](https://stratadb.org/e/invalid_argument.executor.vector_input)
- [`failed_precondition.engine.embedding_model_missing`](https://stratadb.org/e/failed_precondition.engine.embedding_model_missing)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.model_load_failed`](https://stratadb.org/e/inference.model_load_failed)
- [`inference.local_runtime_failed`](https://stratadb.org/e/inference.local_runtime_failed)
- [`inference.registry_corrupt`](https://stratadb.org/e/inference.registry_corrupt)
- [`inference.missing_api_key`](https://stratadb.org/e/inference.missing_api_key)
- [`inference.provider_auth_failed`](https://stratadb.org/e/inference.provider_auth_failed)
- [`inference.provider_unavailable`](https://stratadb.org/e/inference.provider_unavailable)
- [`inference.provider_timeout`](https://stratadb.org/e/inference.provider_timeout)
- [`inference.provider_rate_limited`](https://stratadb.org/e/inference.provider_rate_limited)
- [`inference.provider_quota_exhausted`](https://stratadb.org/e/inference.provider_quota_exhausted)
- [`inference.provider_model_not_found`](https://stratadb.org/e/inference.provider_model_not_found)
- [`inference.provider_malformed_response`](https://stratadb.org/e/inference.provider_malformed_response)
- [`inference.unsupported_provider`](https://stratadb.org/e/inference.unsupported_provider)
- [`inference.unsupported_parameter`](https://stratadb.org/e/inference.unsupported_parameter)

## Invocation

- CLI: `strata vector query`
- Wire type: `vector_query`
