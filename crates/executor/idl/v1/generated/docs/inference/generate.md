---
title: "Generate text"
description: "Generate text with an inference model."
source: strata-core@1.2.1
section: inference
---

Runs a text-generation request against a local or cloud model and returns the completion, the reason generation stopped, and the provider-reported prompt and completion token counts. The request controls the maximum completion tokens, sampling temperature, top-k and top-p cutoffs, an optional deterministic seed, string and token-id stop sequences, and an optional GBNF grammar for constrained generation. Chat models expect their chat template already applied in the prompt. Local models require a build with the local execution feature; cloud providers require the matching provider feature and an API key.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | yes | Model spec. |
| `frequency_penalty` | `number` | no | Frequency penalty. |
| `grammar` | `string` | no | GBNF grammar for constrained generation (local). |
| `logit_bias` | `object` | no | Per-token logit bias (token id → bias). |
| `logprobs` | `boolean` | no | Whether to return log-probabilities. |
| `max_tokens` | `integer` | no | Maximum completion tokens. |
| `messages` | `ChatMessage[]` | no | Chat messages (system/user/assistant/tool). |
| `min_p` | `number` | no | Min-p sampling cutoff. |
| `mirostat` | `Mirostat` | no | Mirostat sampling. |
| `model_config` | `ModelConfig` | no | Per-model load/context configuration. |
| `presence_penalty` | `number` | no | Presence penalty. |
| `prompt` | `string` | no | Raw completion prompt (base models / full control). |
| `repeat_last_n` | `integer` | no | Repetition penalty look-back window. |
| `repeat_penalty` | `number` | no | Repetition penalty. |
| `response_format` | `ResponseFormat` | no | Output format constraint. |
| `seed` | `integer` | no | Deterministic sampling seed. |
| `stop` | `string[]` | no | Stop sequences. |
| `stop_token_ids` | `integer[]` | no | Token-id stop sequences (local). |
| `temperature` | `number` | no | Sampling temperature. |
| `tfs_z` | `number` | no | Tail-free sampling z. |
| `tool_choice` | `ToolChoice` | no | How the model should choose among `tools`. |
| `tools` | `Tool[]` | no | Tools (functions) the model may call. |
| `top_k` | `integer` | no | Top-k sampling cutoff. |
| `top_logprobs` | `integer` | no | Number of top log-probabilities to return per token. |
| `top_p` | `number` | no | Nucleus sampling cutoff. |
| `typical_p` | `number` | no | Typical-p (locally typical) sampling. |

## Returns

`ChatResponse`.

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

- CLI: `strata inference generate`
- Wire type: `inference_generate`
