---
title: "Embed one or more texts"
description: "Embed one or more texts into vectors."
source: strata-core@1.2.1
section: inference
---

Embeds text with an embedding-capable model and returns one vector per input, in order. The `input` field takes either a single string or an array of strings, so single and batch embedding share one command. The vector dimension is fixed by the model. Local embedding models require a build with the local execution feature; cloud embedding providers (OpenAI, Google) require the matching provider feature and an API key.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | yes | Model spec. |
| `dimensions` | `integer` | no | Truncate to this many dimensions (matryoshka), then renormalize. |
| `input` | `EmbedInput` | yes | Text(s) to embed. |
| `input_type` | `InputType` | no | Query vs document role for instruction-tuned embedders. |
| `instruction` | `string` | no | Explicit instruction prefix override. |
| `normalize` | `boolean` | no | Force L2 normalization on/off (default per-model). |

## Returns

`EmbeddingsResponse`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
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

- CLI: `strata inference embed`
- Wire type: `inference_embed`
