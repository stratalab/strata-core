---
title: "Detokenize token ids"
description: "Detokenize token ids with a local model."
source: strata-core@1.2.1
section: inference
---

Decodes an ordered list of token ids back into text using a local model's vocabulary, returning the reconstructed string. Detokenization is a local-only operation: it requires a build with the local execution feature and returns `inference.unsupported_operation` for cloud provider specs.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `ids` | `integer[]` | yes | Token ids. |
| `model` | `string` | yes | Model spec. |

## Returns

`DetokenizedText`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.unknown_model`](https://stratadb.org/e/inference.unknown_model)
- [`inference.model_load_failed`](https://stratadb.org/e/inference.model_load_failed)
- [`inference.local_runtime_failed`](https://stratadb.org/e/inference.local_runtime_failed)
- [`inference.registry_corrupt`](https://stratadb.org/e/inference.registry_corrupt)

## Invocation

- CLI: `strata inference detokenize`
- Wire type: `inference_detokenize`
