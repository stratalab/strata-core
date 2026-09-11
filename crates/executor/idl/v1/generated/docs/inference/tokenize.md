---
title: "Tokenize text"
description: "Tokenize text with a local model."
source: strata-core@1.2.1
section: inference
---

Encodes text into the token id sequence a local model would see and returns the ids in order. Set `add_special` to include the model's special tokens (such as beginning-of-sequence markers). Tokenization is a local-only operation: it requires a build with the local execution feature and returns `inference.unsupported_operation` for cloud provider specs.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `add_special` | `boolean` | no | Whether to add special tokens. |
| `model` | `string` | yes | Model spec. |
| `text` | `string` | yes | Text to tokenize. |

## Returns

`integer[]`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.unknown_model`](https://stratadb.org/e/inference.unknown_model)
- [`inference.io_failure`](https://stratadb.org/e/inference.io_failure)
- [`inference.model_load_failed`](https://stratadb.org/e/inference.model_load_failed)
- [`inference.local_runtime_failed`](https://stratadb.org/e/inference.local_runtime_failed)
- [`inference.registry_corrupt`](https://stratadb.org/e/inference.registry_corrupt)

## Invocation

- CLI: `strata inference tokenize`
- Wire type: `inference_tokenize`
