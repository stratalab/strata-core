---
title: "Update vector embedding"
description: "Replace the embedding for one vector, keeping its metadata."
source: strata-core@1.2.4
section: vector
---

Replaces one visible vector's embedding and leaves its metadata as it stands — the mirror of `vector update-metadata`, which patches metadata and leaves the embedding. Use this to re-embed a document after a model change, a re-chunk or a backfill; `vector upsert` writes the whole record, so metadata not restated there is removed. Missing vectors return a no-op mutation acknowledgement and are never created. With `text` instead of a vector, the text is embedded through the collection's recorded model on the same inference path as `vector upsert`, so the `inference.*` failures listed below apply only to that form.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Re-embed a vector without restating its metadata.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a '[1.0,0.0,0.0]' --metadata '{"lang":"en","tier":1}'
created a in docs
$ strata vector update-embedding docs a '[0.0,1.0,0.0]'
updated a in docs
$ strata vector query docs '[0.0,1.0,0.0]' --k 1
KEY  SCORE  METADATA
a      1.0  {"lang":"en","tier":1}
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","metadata":{"lang":"en","tier":1},"type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"a","type":"vector_update_embedding","vector":[0.0,1.0,0.0]}
{"collection":"docs","k":1,"query":[0.0,1.0,0.0],"type":"vector_query"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `key` | `string` | yes | Vector key. |
| `text` | `string` | no | Text to embed with the collection's recorded model, instead of supplying a vector. Exactly one of `vector` or `text`. |
| `vector` | `number[]` | no | Dense embedding. Accepted at wire (f64) precision and narrowed to the stored f32; a value that underflows or overflows f32 is rejected. Empty when `text` is supplied instead, on the same terms as `VectorUpsert`: a vector carries no model, so supplying one is the caller's statement that the collection's recorded model produced it. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_dimension`](https://stratadb.org/e/invalid_argument.engine.vector_dimension)
- [`invalid_argument.engine.vector_embedding`](https://stratadb.org/e/invalid_argument.engine.vector_embedding)
- [`invalid_argument.executor.vector_dimension`](https://stratadb.org/e/invalid_argument.executor.vector_dimension)
- [`invalid_argument.executor.vector_input`](https://stratadb.org/e/invalid_argument.executor.vector_input)
- [`failed_precondition.engine.embedding_model_missing`](https://stratadb.org/e/failed_precondition.engine.embedding_model_missing)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.unknown_model`](https://stratadb.org/e/inference.unknown_model)
- [`inference.io_failure`](https://stratadb.org/e/inference.io_failure)
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

- CLI: `strata vector update-embedding`
- Wire type: `vector_update_embedding`
