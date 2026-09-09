---
title: "Upsert vector"
description: "Insert or replace one vector."
source: strata-core@1.2.1
section: vector
---

Upserts one vector key with a dense embedding and optional metadata. The vector dimension must match the collection configuration. With `text` instead of a vector, the text is embedded through the collection's recorded model on the same inference path as `inference.embed`, so the `inference.*` failures listed below apply only to that form; a supplied vector never reaches the inference runtime.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Insert or replace a vector with optional metadata.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
$ strata vector upsert docs a [1.0,0.0,0.0] --metadata {"tag":"x"}
$ strata vector exists docs a
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","metadata":{"tag":"x"},"type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"a","type":"vector_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `key` | `string` | yes | Vector key. |
| `metadata` | `any` | no | Optional metadata. |
| `text` | `string` | no | Text to embed with the collection's recorded model, instead of supplying a vector (D10). Exactly one of `vector` or `text`. |
| `vector` | `number[]` | no | Dense embedding. Accepted at wire (f64) precision and narrowed to the stored f32; a value that underflows or overflows f32 is rejected. Empty when `text` is supplied instead. A vector carries no model, so when the collection records an embedding model, Strata cannot check this vector against it: supplying one is the caller's statement that the recorded model produced it. Only `text` is embedded under the record. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck<VectorWrite>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_dimension`](https://stratadb.org/e/invalid_argument.engine.vector_dimension)
- [`invalid_argument.engine.vector_embedding`](https://stratadb.org/e/invalid_argument.engine.vector_embedding)
- [`invalid_argument.engine.vector_metadata`](https://stratadb.org/e/invalid_argument.engine.vector_metadata)
- [`invalid_argument.executor.vector_dimension`](https://stratadb.org/e/invalid_argument.executor.vector_dimension)
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

- CLI: `strata vector upsert`
- Wire type: `vector_upsert`
