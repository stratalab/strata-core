---
title: "Report model capabilities"
description: "Report capabilities for a model spec."
source: strata-core@1.2.1
section: inference
---

Parses a model spec into a provider and model name and reports what that combination supports without running the model. The result states whether generation, tokenization, embedding, and ranking are available, whether the operation requires network access or an API key, whether this binary was compiled with the provider feature needed to execute, whether the runtime currently permits network calls, and the known embedding dimension. Model specs are catalog names (`tinyllama`), catalog `name:quant` pairs (`tinyllama:q8_0`), local GGUF paths, or provider specs (`anthropic:claude-...`).

## Examples

Report a model's capabilities without a network call.

### CLI

```console
$ strata inference capability openai:gpt-4o-mini  # Pure metadata — no request is sent to the provider.
provider                  openai
model                     gpt-4o-mini
availability              ready
can_generate              true
can_embed                 true
can_rank                  false
can_tokenize              false
embedding_dim             0
requires_api_key          true
requires_network          true
network_enabled           true
provider_feature_enabled  true
supports_tools            true
supports_json_object      true
supports_json_schema      true
supports_logprobs         true
```

### Wire

```json
{"model":"openai:gpt-4o-mini","type":"inference_model_capability"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | yes | Model spec. |

## Returns

`InferenceCapability`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.io_failure`](https://stratadb.org/e/inference.io_failure)

## Invocation

- CLI: `strata inference capability`
- Wire type: `inference_model_capability`
